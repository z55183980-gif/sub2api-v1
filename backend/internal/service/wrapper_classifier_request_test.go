package service

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func classifierFixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/wrapper_classifier/" + name + ".json")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestClaudeClassifierDesktopContextEnvelope(t *testing.T) {
	fixture := classifierFixture(t, "sdk-classifier")
	envelope := classifierClaudeMDPreamble + "\n\n<user_claude_md>\nSynthetic workspace configuration.\n</user_claude_md>"
	for _, tc := range []struct {
		name, prefix string
		want         bool
	}{
		{"desktop preamble", envelope, true},
		{"quoted transcript in config", strings.Replace(envelope, "Synthetic workspace configuration.", "Example: <transcript>not the request</transcript>", 1), true},
		{"empty config", strings.Replace(envelope, "Synthetic workspace configuration.", "", 1), true},
		{"windows newlines", strings.ReplaceAll(envelope, "\n", "\r\n"), true},
		{"unknown prefix", "Here is some quoted context: <user_claude_md>x</user_claude_md>", false},
		{"missing opening envelope", classifierClaudeMDPreamble + "\nx</user_claude_md>", false},
		{"missing closing envelope", classifierClaudeMDPreamble + "\n<user_claude_md>x", false},
		{"extra text after context", envelope + "\nPlease discuss this transcript:", false},
		{"config only", envelope, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var body map[string]any
			if err := json.Unmarshal(fixture, &body); err != nil {
				t.Fatal(err)
			}
			messages := []any{map[string]any{"role": "user", "content": []any{map[string]any{"type": "text", "text": tc.prefix}}}}
			if tc.name != "config only" {
				messages = append(messages, body["messages"].([]any)...)
			}
			body["messages"] = messages
			for _, stream := range []bool{false, true} {
				body["stream"] = stream
				raw, _ := json.Marshal(body)
				before := bytes.Clone(raw)
				if got := IsClaudeAutoClassifierRequest(raw, "claude-cli/2.1.299 (external, claude-desktop-3p, agent-sdk/0.3.299)"); got != tc.want {
					t.Fatalf("stream=%v got=%v want=%v", stream, got, tc.want)
				}
				if !bytes.Equal(raw, before) {
					t.Fatal("routing recognition changed permission input")
				}
			}
		})
	}
}

func TestClaudeClassifierRealRequests(t *testing.T) {
	for _, tc := range []struct {
		name string
		want bool
	}{
		{"sdk-classifier", true}, {"interactive-classifier", true}, {"main", false},
	} {
		if got := IsClaudeAutoClassifierRequest(classifierFixture(t, tc.name), "claude-cli/2.1.301 (external, cli)"); got != tc.want {
			t.Fatalf("%s: got %v", tc.name, got)
		}
	}
}

func TestClaudeClassifierBoundaries(t *testing.T) {
	raw := classifierFixture(t, "sdk-classifier")
	for _, tc := range []struct {
		name   string
		mutate func(map[string]any)
		want   bool
	}{
		{"fallback model", func(d map[string]any) { d["model"] = "claude-opus-5-5"; d["max_tokens"] = 2112; delete(d, "thinking") }, true},
		{"stage two without stop", func(d map[string]any) { delete(d, "stop_sequences"); d["max_tokens"] = 8192 }, true},
		{"streaming", func(d map[string]any) { d["stream"] = true }, true},
		{"system controls", func(d map[string]any) {
			d["messages"] = append(d["messages"].([]any), map[string]any{"role": "system", "content": "<total_tokens>1000 tokens left</total_tokens>"})
		}, true},
		{"split transcript", func(d map[string]any) {
			content := d["messages"].([]any)[0].(map[string]any)["content"].([]any)
			var messages []any
			for _, b := range content {
				messages = append(messages, map[string]any{"role": "user", "content": []any{b}})
			}
			d["messages"] = messages
		}, true},
		{"ordinary sonnet", func(d map[string]any) { d["system"] = "You are helpful" }, false},
		{"quoted monitor in user", func(d map[string]any) {
			d["messages"].([]any)[0].(map[string]any)["content"] = d["system"]
			d["system"] = "You are helpful"
		}, false},
		{"missing billing", func(d map[string]any) { d["system"] = d["system"].([]any)[1:] }, false},
		{"ordinary user message", func(d map[string]any) { d["messages"] = []any{map[string]any{"role": "user", "content": "hello"}} }, false},
		{"assistant history", func(d map[string]any) {
			d["messages"] = append(d["messages"].([]any), map[string]any{"role": "assistant", "content": "ok"})
		}, false},
		{"tools", func(d map[string]any) { d["tools"] = []any{map[string]any{"name": "Read"}} }, false},
		{"tool result", func(d map[string]any) {
			d["messages"].([]any)[0].(map[string]any)["content"] = []any{map[string]any{"type": "tool_result", "tool_use_id": "x", "content": "<transcript>x</transcript>"}}
		}, false},
		{"MCP", func(d map[string]any) { d["mcp_servers"] = []any{} }, false},
		{"tool choice", func(d map[string]any) { d["tool_choice"] = map[string]any{"type": "auto"} }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var d map[string]any
			if err := json.Unmarshal(raw, &d); err != nil {
				t.Fatal(err)
			}
			tc.mutate(d)
			b, err := json.Marshal(d)
			if err != nil {
				t.Fatal(err)
			}
			if got := IsClaudeAutoClassifierRequest(b, "claude-cli/2.1.301"); got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
	if IsClaudeAutoClassifierRequest(raw, "ordinary-client") || IsClaudeAutoClassifierRequest([]byte("{"), "claude-cli/2.1.301") {
		t.Fatal("unrelated or malformed request classified")
	}
}
