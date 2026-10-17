package service

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/tidwall/gjson"
)

// Captured from real Claude Code 2.1.300: three Edit + one Read, then two
// Edit calls. The CLI inserts replace_all:false after executing each Edit.
func TestManagedHistoryRealCLIEditDefaults(t *testing.T) {
	data, err := os.ReadFile("testdata/wrapper_history/cli_2_1_300_edit_defaults.json")
	if err != nil {
		t.Fatal(err)
	}
	var captures []struct {
		Events            []json.RawMessage `json:"events"`
		ReturnedAssistant json.RawMessage   `json:"returned_assistant"`
	}
	if err := json.Unmarshal(data, &captures); err != nil {
		t.Fatal(err)
	}
	for _, capture := range captures {
		for _, compact := range []bool{false, true} {
			ctx := context.Background()
			store := newWrapperManagedTestStore()
			key := WrapperManagedAutoKey(42, "real-edit-defaults")
			first, err := beginWrapperManagedAutoTurn(ctx, store, key)
			if err != nil {
				t.Fatal(err)
			}
			user := json.RawMessage(`{"role":"user","content":"perform the edits"}`)
			if _, active, err := first.Prepare(ctx, 5, managedAlignmentRequest([]json.RawMessage{user}), managedTestContext()); err != nil || !active {
				t.Fatalf("prepare: %v %v", active, err)
			}
			native := first.state.NativeSessionID
			for _, event := range capture.Events {
				first.Observe(event)
			}
			first.Observe([]byte(`{"type":"message_stop","wrapper_turn_id":"11111111-1111-4111-8111-111111111111"}`))
			if err := first.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			if compact {
				first.state.Messages = nil
			} else {
				first.state.Proofs = nil
			}
			if err := first.save(ctx); err != nil {
				t.Fatal(err)
			}
			first.Release()
			var results []map[string]any
			for _, block := range gjson.GetBytes(capture.ReturnedAssistant, "content").Array() {
				if block.Get("type").String() == "tool_use" {
					results = append(results, map[string]any{"type": "tool_result", "tool_use_id": block.Get("id").String(), "content": "ok"})
				}
			}
			result, _ := json.Marshal(map[string]any{"role": "user", "content": results})
			second, err := beginWrapperManagedAutoTurn(ctx, store, key)
			if err != nil {
				t.Fatal(err)
			}
			c := managedTestContext()
			wire, active, err := second.Prepare(ctx, 5, managedAlignmentRequest([]json.RawMessage{user, capture.ReturnedAssistant, result}), c)
			second.Release()
			if err != nil || !active || second.skipReason != "" || second.state.NativeSessionID != native || c.GetHeader("X-Wrapper-Ack-Turn-Id") == "" || gjson.GetBytes(wire, "messages.#").Int() != 1 {
				t.Errorf("real CLI continuation rebuilt: compact=%v reason=%q active=%v error=%v", compact, second.skipReason, active, err)
			}
		}
	}
}

func TestManagedHistoryEditDefaultsPreserveChanges(t *testing.T) {
	base := json.RawMessage(`{"role":"assistant","content":[{"type":"tool_use","id":"tool-1","name":"Edit","input":{"file_path":"probe.txt","old_string":"old","new_string":"new"}}]}`)
	for _, changed := range []string{
		strings.Replace(string(base), `"tool-1"`, `"tool-2"`, 1),
		strings.Replace(string(base), `"name":"Edit"`, `"name":"OtherEdit"`, 1),
	} {
		if equalManagedMessage(base, json.RawMessage(changed)) || managedProof(base).Digest == managedProof(json.RawMessage(changed)).Digest {
			t.Fatal("changed tool identity was accepted")
		}
	}
	for _, tc := range []struct {
		name, input string
		same        bool
	}{
		{"default false", `{"file_path":"probe.txt","old_string":"old","new_string":"new","replace_all":false}`, true},
		{"replace all", `{"file_path":"probe.txt","old_string":"old","new_string":"new","replace_all":true}`, false},
		{"null", `{"file_path":"probe.txt","old_string":"old","new_string":"new","replace_all":null}`, false},
		{"string false", `{"file_path":"probe.txt","old_string":"old","new_string":"new","replace_all":"false"}`, false},
		{"changed text", `{"file_path":"probe.txt","old_string":"old","new_string":"other","replace_all":false}`, false},
		{"changed path", `{"file_path":"other.txt","old_string":"old","new_string":"new","replace_all":false}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			actual := json.RawMessage(`{"role":"assistant","content":[{"type":"tool_use","id":"tool-1","name":"Edit","input":` + tc.input + `}]}`)
			if got := equalManagedMessage(base, actual); got != tc.same {
				t.Fatalf("same=%v want %v", got, tc.same)
			}
			if got := managedProof(base).Digest == managedProof(actual).Digest; got != tc.same {
				t.Fatalf("proof same=%v want %v", got, tc.same)
			}
		})
	}
	for _, name := range []string{"custom_edit", "mcp__external__Edit", "Read"} {
		a, _ := json.Marshal(map[string]any{"role": "assistant", "content": []any{map[string]any{"type": "tool_use", "id": "tool-1", "name": name, "input": map[string]any{"file_path": "probe.txt", "old_string": "old", "new_string": "new"}}}})
		var obj map[string]any
		json.Unmarshal(a, &obj)
		obj["content"].([]any)[0].(map[string]any)["input"].(map[string]any)["replace_all"] = false
		b, _ := json.Marshal(obj)
		if equalManagedMessage(a, b) {
			t.Errorf("custom tool %s default was ignored", name)
		}
	}
}
