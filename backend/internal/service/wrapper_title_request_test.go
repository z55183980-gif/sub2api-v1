package service

import (
	"encoding/json"
	"os"
	"testing"
)

func TestClaudeTitleRealRequests(t *testing.T) {
	for _, tc := range []struct {
		name string
		want bool
	}{{"title", true}, {"main", false}} {
		b, err := os.ReadFile("testdata/wrapper_title/" + tc.name + ".json")
		if err != nil {
			t.Fatal(err)
		}
		if got := IsClaudeSessionTitleRequest(b, "claude-cli/2.1.300 (external, cli)"); got != tc.want {
			t.Fatalf("%s: %v", tc.name, got)
		}
	}
}
func TestClaudeTitleRejectsAmbiguousRequests(t *testing.T) {
	raw, _ := os.ReadFile("testdata/wrapper_title/title.json")
	tests := map[string]func(map[string]any){
		"ordinary system": func(d map[string]any) { d["system"] = "Return a JSON title" },
		"tools":           func(d map[string]any) { d["tools"] = []any{map[string]any{"name": "Read"}} },
		"tool result": func(d map[string]any) {
			d["messages"] = []any{map[string]any{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": "x", "content": "<session>x</session>"}}}}
		},
		"history": func(d map[string]any) {
			d["messages"] = append(d["messages"].([]any), map[string]any{"role": "assistant", "content": "hello"})
		},
		"ordinary JSON schema": func(d map[string]any) {
			d["output_config"] = map[string]any{"format": map[string]any{"type": "json_schema", "schema": map[string]any{"type": "object"}}}
		},
		"missing format": func(d map[string]any) { delete(d, "output_config") },
		"MCP":            func(d map[string]any) { d["metadata"].(map[string]any)["mcp_servers"] = []any{} },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			var d map[string]any
			json.Unmarshal(raw, &d)
			mutate(d)
			b, _ := json.Marshal(d)
			if IsClaudeSessionTitleRequest(b, "claude-cli/2.1.300") {
				t.Fatal("misclassified")
			}
		})
	}
	if IsClaudeSessionTitleRequest(raw, "ordinary-client") {
		t.Fatal("non CLI matched")
	}
}
