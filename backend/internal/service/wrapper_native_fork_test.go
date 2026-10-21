package service

import (
	"encoding/json"
	"github.com/tidwall/gjson"
	"testing"
)

func TestPrepareNativeForkPreservesParent(t *testing.T) {
	history := []json.RawMessage{json.RawMessage(`{"role":"user","content":"seed"}`), json.RawMessage(`{"role":"assistant","content":"OK"}`)}
	state := wrapperManagedState{AccountID: 6, NativeSessionID: "sub2api-parent", Model: "sonnet", Receipt: "receipt", Phase: "ready", Proofs: managedProofs(history)}
	body := []byte(`{"model":"sonnet","messages":[{"role":"user","content":"seed"},{"role":"assistant","content":"OK"},{"role":"user","content":"recap"}],"tools":[{"name":"Bash"}]}`)
	for _, variant := range []string{"valid", "changed", "pending", "model", "stale"} {
		t.Run(variant, func(t *testing.T) {
			turn := &WrapperManagedAutoTurn{state: state}
			input := body
			switch variant {
			case "changed":
				input = []byte(`{"model":"sonnet","messages":[{"role":"user","content":"wrong"},{"role":"assistant","content":"OK"},{"role":"user","content":"recap"}]}`)
			case "pending":
				turn.state.PendingToolIDs = []string{"tool"}
			case "model":
				turn.state.Model = "opus"
			case "stale":
				turn.state.Phase = "pending"
			}
			before, _ := json.Marshal(turn.state)
			c := managedTestContext()
			out, active, err := turn.PrepareNativeFork(input, c)
			if err != nil {
				t.Fatal(err)
			}
			after, _ := json.Marshal(turn.state)
			if string(before) != string(after) {
				t.Fatal("parent mutated")
			}
			if active != (variant == "valid") {
				t.Fatalf("active=%v", active)
			}
			if active {
				if gjson.GetBytes(out, "messages.#").Int() != 1 || gjson.GetBytes(out, "tools").Exists() || c.GetHeader("X-Wrapper-Fork-Parent") != "sub2api-parent" {
					t.Fatalf("bad fork %s", out)
				}
			} else if c.GetHeader("X-Wrapper-Fork-Parent") != "" {
				t.Fatal("unsafe fork header")
			}
		})
	}
}
