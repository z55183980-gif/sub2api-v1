import importlib.util
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
import sys
from types import SimpleNamespace


with patch.dict(sys.modules, {"fcntl": SimpleNamespace(flock=lambda *_: None, LOCK_EX=2)}):
    spec = importlib.util.spec_from_file_location("vm_access", Path(__file__).with_name("s4-local-vm-access.py"))
    vm_access = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(vm_access)


class VMAccessTest(unittest.TestCase):
    def test_created_wrapper_registration_controls_access(self):
        with TemporaryDirectory() as directory, patch.object(vm_access, "DATA_DIR", Path(directory)):
            data = Path(directory)
            (data / "vms.json").write_text(json.dumps({"vms": [
                {"id": "new", "source": "created", "sshHost": "192.168.122.222"},
                {"id": "manual", "source": "imported", "sshHost": "192.168.122.223"},
            ]}))
            (data / "instances.json").write_text(json.dumps({"instances": [
                {"vmId": "new", "mode": "remote", "host": "192.168.122.222", "port": 4523},
                {"vmId": "manual", "mode": "remote", "host": "192.168.122.223", "port": 4523},
            ]}))
            self.assertEqual(vm_access.desired_ips(), ["192.168.122.222"])
            (data / "instances.json").write_text(json.dumps({"instances": []}))
            self.assertEqual(vm_access.desired_ips(), [])

    def test_rejects_outside_libvirt_subnet(self):
        with TemporaryDirectory() as directory, patch.object(vm_access, "DATA_DIR", Path(directory)):
            data = Path(directory)
            (data / "vms.json").write_text(json.dumps({"vms": [
                {"id": "bad", "source": "created", "sshHost": "198.51.100.42"},
            ]}))
            (data / "instances.json").write_text(json.dumps({"instances": [
                {"vmId": "bad", "mode": "remote", "host": "198.51.100.42", "port": 4523},
            ]}))
            with self.assertRaises(ValueError):
                vm_access.desired_ips()


if __name__ == "__main__":
    unittest.main()
