package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/tidwall/gjson"
	"strings"
	"testing"
)

func TestWrapperManagedImageCLIAndDesktopContinuation(t *testing.T) {
	for _, desktop := range []bool{false, true} {
		t.Run(fmt.Sprint(desktop), func(t *testing.T) {
			ctx := context.Background()
			store := newWrapperManagedTestStore()
			key := WrapperManagedAutoKey(42, fmt.Sprint("images-", desktop))
			user := json.RawMessage(`{"role":"user","content":"read image"}`)
			first, _ := beginWrapperManagedAutoTurn(ctx, store, key)
			if _, active, err := first.Prepare(ctx, 6, managedAlignmentRequest([]json.RawMessage{user}), managedTestContext()); err != nil || !active {
				t.Fatalf("start: %v", err)
			}
			native := first.state.NativeSessionID
			observeManagedAlignmentTurn(first, "reading", "toolu_image", 1)
			if err := first.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			first.Release()
			// >2 MiB encoded image forces Redis proof-only compaction on commit.
			data := strings.Repeat("AAAA", 700000)
			result := json.RawMessage(fmt.Sprintf(`{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_image","content":[{"type":"text","text":"before"},{"type":"image","source":{"type":"base64","media_type":"image/png","data":%q}}]}]}`, data))
			history := []json.RawMessage{user, managedAlignmentAssistant("reading", "toolu_image"), result}
			if desktop {
				history = append(history, managedBudgetReminder(99))
			}
			second, _ := beginWrapperManagedAutoTurn(ctx, store, key)
			c := managedTestContext()
			invalid := append([]json.RawMessage(nil), history...)
			invalid[2] = json.RawMessage(strings.Replace(string(result), data, "bad!", 1))
			if _, _, err := second.Prepare(ctx, 6, managedAlignmentRequest(invalid), c); err == nil {
				t.Fatal("invalid image accepted")
			}
			if second.state.NativeSessionID != native || second.state.Phase != "ready" {
				t.Fatal("invalid image destroyed pending session")
			}
			wire, active, err := second.Prepare(ctx, 6, managedAlignmentRequest(history), c)
			if err != nil || !active {
				t.Fatalf("image continuation: %v", err)
			}
			if reason, _, _ := second.Decision(); reason != "" {
				t.Fatal(reason)
			}
			if second.state.NativeSessionID != native {
				t.Fatal("image reset session")
			}
			parts := gjson.GetBytes(wire, "messages.0.content.0.content").Array()
			if len(parts) < 2 || parts[1].Get("source.data").String() != data {
				t.Fatal("image missing or changed")
			}
			if desktop && (len(parts) != 3 || !strings.Contains(parts[2].Get("text").String(), "99")) {
				t.Fatal("desktop reminder/order lost")
			}
			observeManagedAlignmentTurn(second, "next", "toolu_next", 2)
			if err := second.Commit(ctx); err != nil {
				t.Fatal(err)
			}
			second.Release()
			if len(store.states[key]) > 10000 {
				t.Fatal("image retained in durable state instead of proof")
			}
			history = append(history, managedAlignmentAssistant("next", "toolu_next"), managedAlignmentToolResult("toolu_next"))
			third, _ := beginWrapperManagedAutoTurn(ctx, store, key)
			defer third.Release()
			next, active, err := third.Prepare(ctx, 6, managedAlignmentRequest(history), managedTestContext())
			if err != nil || !active || third.state.NativeSessionID != native {
				t.Fatalf("history reuse failed: %v", err)
			}
			if strings.Contains(string(next), data) || gjson.GetBytes(next, "messages.#").Int() != 1 {
				t.Fatal("historical image retransmitted")
			}
		})
	}
}

func TestWrapperManagedImageValidation(t *testing.T) {
	for _, source := range []string{`{"type":"url","url":"https://example.com/a.png"}`, `{"type":"base64","media_type":"image/png","data":"bad!"}`, `{"type":"base64","media_type":"application/pdf","data":"AAAA"}`} {
		var result map[string]any
		json.Unmarshal([]byte(`{"content":[{"type":"image","source":`+source+`}]}`), &result)
		if validManagedToolResult(result) != "invalid_tool_result_image" {
			t.Fatal("invalid image accepted")
		}
	}
}
