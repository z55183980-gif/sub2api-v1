package service

import (
	"encoding/json"
	"testing"
)

func TestClaudeSessionRecapClassification(t *testing.T) {
	message := func(role string, content any) map[string]any { return map[string]any{"role": role, "content": content} }
	block := func(text string) any { return []any{map[string]any{"type": "text", "text": text}} }
	notice := "<system_instruction>\n<total_tokens>123 tokens left</total_tokens>\n</system_instruction>"
	for _, tc := range []struct {
		name  string
		turns []any
		ua    string
		want  bool
	}{
		{"string", []any{message("user", claudeSessionRecapInstruction)}, "claude-cli/2.1.302", true},
		{"blocks", []any{message("user", block(claudeSessionRecapInstruction))}, "claude-cli/2.1.302", true},
		{"token notice", []any{message("user", claudeSessionRecapInstruction), message("user", block(notice))}, "claude-cli/2.1.302", true},
		{"ordinary", []any{message("user", "Please recap our work")}, "claude-cli/2.1.302", false},
		{"quoted", []any{message("user", "Explain: "+claudeSessionRecapInstruction)}, "claude-cli/2.1.302", false},
		{"extra task", []any{message("user", claudeSessionRecapInstruction+" Also fix a bug.")}, "claude-cli/2.1.302", false},
		{"history", []any{message("user", claudeSessionRecapInstruction), message("assistant", "summary"), message("user", "continue")}, "claude-cli/2.1.302", false},
		{"assistant", []any{message("assistant", claudeSessionRecapInstruction)}, "claude-cli/2.1.302", false},
		{"unknown tail", []any{message("user", claudeSessionRecapInstruction), message("system", "extra")}, "claude-cli/2.1.302", false},
		{"bad token notice", []any{message("user", claudeSessionRecapInstruction), message("user", notice+" continue")}, "claude-cli/2.1.302", false},
		{"tool result", []any{message("user", []any{map[string]any{"type": "tool_result", "content": claudeSessionRecapInstruction}})}, "claude-cli/2.1.302", false},
		{"mixed blocks", []any{message("user", []any{map[string]any{"type": "text", "text": claudeSessionRecapInstruction}, map[string]any{"type": "tool_result", "content": "result"}})}, "claude-cli/2.1.302", false},
		{"other client", []any{message("user", claudeSessionRecapInstruction)}, "other/1", false},
		{"empty", nil, "claude-cli/2.1.302", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body, _ := json.Marshal(map[string]any{"messages": tc.turns})
			if got := IsClaudeSessionRecapRequest(body, tc.ua); got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
	if IsClaudeSessionRecapRequest([]byte(`{"messages":`), "claude-cli/2.1.302") {
		t.Fatal("invalid JSON recognized")
	}
}
