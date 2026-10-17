package service

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

func bashReconcileMessage(command string) json.RawMessage {
	b, _ := json.Marshal(map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "tool_use", "id": "tool-bash", "name": "Bash", "input": map[string]any{"command": command, "description": "diagnostic"}}}})
	return b
}

func TestManagedPendingBashReconciliation(t *testing.T) {
	for _, agent := range []string{
		"claude-cli/2.1.299 (external, claude-desktop-3p, agent-sdk/0.3.299)",
		"claude-cli/2.1.299 (external, cli)",
		"claude-cli/2.1.301 (external, cli)",
	} {
		for _, compact := range []bool{false, true} {
			t.Run(agent+"/"+map[bool]string{false: "messages", true: "proofs"}[compact], func(t *testing.T) {
				ctx := context.Background()
				store := newWrapperManagedTestStore()
				key := WrapperManagedAutoKey(42, "bash-reconcile")
				user := json.RawMessage(`{"role":"user","content":"run diagnostic"}`)
				original := bashReconcileMessage("cd /tmp/test && pwd")
				returned := bashReconcileMessage("pwd")
				result := json.RawMessage(`{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool-bash","content":"/tmp/test"}]}`)
				turn, err := beginWrapperManagedAutoTurn(ctx, store, key)
				if err != nil {
					t.Fatal(err)
				}
				body := managedAlignmentRequest([]json.RawMessage{user})
				_, _, err = turn.Prepare(ctx, 5, body, managedTestContext())
				if err != nil {
					t.Fatal(err)
				}
				turn.blocks[0] = json.RawMessage(gjson.GetBytes(original, "content.0").Raw)
				turn.terminal = true
				turn.stopReason = "tool_use"
				turn.receipt = "11111111-1111-4111-8111-111111111111"
				if err = turn.Commit(ctx); err != nil {
					t.Fatal(err)
				}
				native := turn.state.NativeSessionID
				if compact {
					turn.state.Messages = nil
				} else {
					turn.state.Proofs = nil
				}
				if err = turn.save(ctx); err != nil {
					t.Fatal(err)
				}
				turn.Release()
				next, err := beginWrapperManagedAutoTurn(ctx, store, key)
				if err != nil {
					t.Fatal(err)
				}
				c := managedTestContext()
				c.Request.Header.Set("User-Agent", agent)
				input := managedAlignmentRequest([]json.RawMessage{user, returned, result})
				snapshot := string(input)
				wire, active, err := next.Prepare(ctx, 5, input, c)
				if err != nil || !active || next.skipReason != "" || next.state.NativeSessionID != native || c.GetHeader("X-Wrapper-Ack-Turn-Id") == "" {
					t.Fatalf("rebuild: active=%v reason=%s err=%v", active, next.skipReason, err)
				}
				if string(input) != snapshot {
					t.Fatal("mutated request bytes")
				}
				if !strings.Contains(string(wire), "client_reported_command") || !strings.Contains(string(wire), "Execution cwd is unverified") || gjson.GetBytes(wire, "messages.#").Int() != 1 {
					t.Fatal("missing upstream correction")
				}
				next.blocks[0] = json.RawMessage(`{"type":"text","text":"done"}`)
				next.terminal = true
				next.stopReason = "end_turn"
				next.receipt = "22222222-2222-4222-8222-222222222222"
				if err = next.Commit(ctx); err != nil {
					t.Fatal(err)
				}
				next.Release()
				third, err := beginWrapperManagedAutoTurn(ctx, store, key)
				if err != nil {
					t.Fatal(err)
				}
				defer third.Release()
				history := []json.RawMessage{user, returned, result, json.RawMessage(`{"role":"assistant","content":"done"}`), json.RawMessage(`{"role":"user","content":"next"}`)}
				_, active, err = third.Prepare(ctx, 5, managedAlignmentRequest(history), c)
				if err != nil || !active || third.skipReason != "" || third.state.NativeSessionID != native {
					t.Fatalf("later history rebuilt: %v %s", err, third.skipReason)
				}
			})
		}
	}
}

func TestManagedPendingBashReconciliationRejectsOtherChanges(t *testing.T) {
	user := json.RawMessage(`{"role":"user","content":"run diagnostic"}`)
	original := bashReconcileMessage("cd /tmp/test && pwd")
	returned := bashReconcileMessage("pwd")
	result := json.RawMessage(`{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool-bash","content":"ok"}]}`)
	for _, ua := range []string{"claude-cli/2.1.299 (external, claude-desktop-3p)", "claude-cli/2.1.299 (external, cli)"} {
		for _, name := range []string{"command", "id", "name", "description", "earlier_history", "result_id", "missing_result", "no_pending", "other_client", "wrong_recent", "duplicate_result", "old_call"} {
			t.Run(ua+"/"+name, func(t *testing.T) {
				state := wrapperManagedState{Messages: []json.RawMessage{user, original}, PendingToolIDs: []string{"tool-bash"}}
				messages := []json.RawMessage{user, returned, result}
				agent := ua
				switch name {
				case "command":
					messages[1] = bashReconcileMessage("pwd; whoami")
				case "id":
					messages[1] = json.RawMessage(strings.ReplaceAll(string(returned), "tool-bash", "other"))
				case "name":
					messages[1] = json.RawMessage(strings.ReplaceAll(string(returned), `"Bash"`, `"Other"`))
				case "description":
					messages[1] = json.RawMessage(strings.ReplaceAll(string(returned), "diagnostic", "different"))
				case "earlier_history":
					messages[0] = json.RawMessage(`{"role":"user","content":"changed"}`)
				case "result_id":
					messages[2] = json.RawMessage(strings.ReplaceAll(string(result), "tool-bash", "other"))
				case "missing_result":
					messages[2] = user
				case "duplicate_result":
					messages = append(messages, result)
				case "no_pending":
					state.PendingToolIDs = nil
				case "other_client":
					agent = "other-client/2.1.301 (external, cli)"
				case "wrong_recent":
					state.Messages = nil
					state.Proofs = managedProofs([]json.RawMessage{user, original})
					state.Recent = []json.RawMessage{user, returned}
				case "old_call":
					state.Messages = append(state.Messages, result, json.RawMessage(`{"role":"assistant","content":"done"}`))
					messages = append([]json.RawMessage{user, returned, result, json.RawMessage(`{"role":"assistant","content":"done"}`)}, user)
				}
				turn := WrapperManagedAutoTurn{state: state}
				if _, ok := turn.reconcilePendingBash(messages, agent); ok {
					t.Fatal("accepted unrelated change")
				}
			})
		}
	}
	for _, command := range []string{"cd relative && pwd", "cd $HOME && pwd", "cd /tmp/$(id) && pwd", "cd /tmp/test; pwd", "cd /tmp/test && whoami", "cd /tmp/test && cd /else && pwd"} {
		if managedRemovedCD(command, "pwd") {
			t.Fatalf("accepted complex change: %s", command)
		}
	}
	// Ordinary matching remains strict; reconciliation is scoped to a live result.
	if equalManagedMessage(original, returned) || managedProof(original).Digest == managedProof(returned).Digest {
		t.Fatal("global comparison was weakened")
	}
}
