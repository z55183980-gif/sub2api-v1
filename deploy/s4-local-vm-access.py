#!/usr/bin/env python3
"""Reconcile Sub2API's libvirt access with registered Manager Wrappers."""

import ipaddress
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import fcntl

DATA_DIR = Path(os.environ.get("WRAPPER_MANAGER_DATA", "/root/claude-wrapper-test/manager-data"))
STATE_FILE = Path("/run/sub2api-local-s4-vm-access.json")
NETWORK_NAME = "sub2api-local-s4-net"
CHAIN = "LIBVIRT_FWI"
WRAPPER_PORT = 4523
VM_SUBNET = ipaddress.ip_network("192.168.122.0/24")


def run(*args):
    return subprocess.run(args, check=True, capture_output=True, text=True).stdout


def desired_ips():
    vms = json.loads((DATA_DIR / "vms.json").read_text(encoding="utf-8"))["vms"]
    instances = json.loads((DATA_DIR / "instances.json").read_text(encoding="utf-8"))["instances"]
    vm_by_id = {vm["id"]: vm for vm in vms}
    result = set()
    for instance in instances:
        if instance.get("mode") != "remote" or instance.get("port") != WRAPPER_PORT:
            continue
        vm = vm_by_id.get(instance.get("vmId"))
        if not vm or instance.get("host") != vm.get("sshHost"):
            continue
        if vm.get("source") != "created":
            continue
        ip = ipaddress.ip_address(vm["sshHost"])
        if ip not in VM_SUBNET or ip == VM_SUBNET.network_address or ip == VM_SUBNET.broadcast_address:
            raise ValueError("Manager Wrapper address is outside the expected libvirt subnet")
        result.add(str(ip))
    return sorted(result)


def source_cidr():
    config = json.loads(run("docker", "network", "inspect", NETWORK_NAME))
    return str(ipaddress.ip_network(config[0]["IPAM"]["Config"][0]["Subnet"], strict=True))


def rule_args(cidr, ip):
    return ("-s", cidr, "-d", f"{ip}/32", "-o", "virbr0", "-p", "tcp", "--dport", str(WRAPPER_PORT), "-j", "ACCEPT")


def reconcile():
    current = desired_ips()
    cidr = source_cidr()
    old = json.loads(STATE_FILE.read_text(encoding="utf-8")) if STATE_FILE.exists() else {}
    old_ips = old.get("ips", [])
    old_cidr = old.get("cidr", cidr)
    lines = run("iptables", "-S", CHAIN).splitlines()
    reject = next((i for i, line in enumerate(lines) if line.startswith(f"-A {CHAIN} -o virbr0 -j REJECT")), len(lines))
    managed_rules = []
    for index, line in enumerate(lines):
        parts = shlex.split(line)
        if len(parts) < 4 or parts[:2] != ["-A", CHAIN]:
            continue
        try:
            source = parts[parts.index("-s") + 1]
            destination = parts[parts.index("-d") + 1]
            outbound = parts[parts.index("-o") + 1]
            port = parts[parts.index("--dport") + 1]
            target = parts[parts.index("-j") + 1]
            address = ipaddress.ip_network(destination, strict=True)
        except (ValueError, IndexError):
            continue
        if source in (cidr, old_cidr) and outbound == "virbr0" and port == str(WRAPPER_PORT) \
                and target == "ACCEPT" and address.prefixlen == 32 and address.network_address in VM_SUBNET:
            managed_rules.append((index, source, str(address.network_address)))
    valid_rules = len(managed_rules) == len(current) and all(
        sum(1 for index, source, address in managed_rules if index < reject and source == cidr and address == ip) == 1
        for ip in current)
    if old == {"cidr": cidr, "ips": current} and valid_rules:
        return
    # Existing static rules are adopted on first deployment.
    legacy_ips = ["192.168.122.240", "192.168.122.231"] if not STATE_FILE.exists() else []
    discovered = [ip for _, _, ip in managed_rules]
    for ip in sorted(set(old_ips + current + legacy_ips + discovered)):
        for source in set((old_cidr, cidr)):
            args = rule_args(source, ip)
            while subprocess.run(("iptables", "-D", CHAIN, *args), capture_output=True).returncode == 0:
                pass
    # Position 2 is immediately after libvirt's established-connection rule
    # and ahead of its catch-all REJECT.
    for ip in current:
        run("iptables", "-I", CHAIN, "2", *rule_args(cidr, ip))
    temp = STATE_FILE.with_suffix(".tmp")
    temp.write_text(json.dumps({"cidr": cidr, "ips": current}), encoding="utf-8")
    temp.replace(STATE_FILE)
    print(f"Sub2API VM access: {len(current)} registered Wrapper(s)")


def main():
    lock_file = Path("/run/sub2api-local-s4-vm-access.lock")
    with lock_file.open("w") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        reconcile()


if __name__ == "__main__":
    try:
        main()
    except (OSError, ValueError, KeyError, IndexError, subprocess.CalledProcessError, json.JSONDecodeError) as error:
        print(f"Sub2API VM access reconciliation failed: {error}", file=sys.stderr)
        sys.exit(1)
