package service

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

func managedAlignmentAssistant(message string, toolID string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"role":"assistant","content":[{"type":"text","text":%q},{"type":"tool_use","id":%q,"name":"lookup","input":{"item":1}}]}`, message, toolID))
}

func managedAlignmentToolResult(toolID string) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"role":"user","content":[{"type":"tool_result","tool_use_id":%q,"content":"ok"}]}`, toolID))
}

func managedAlignmentRequest(messages []json.RawMessage) []byte {
	data, _ := json.Marshal(map[string]any{"model": "sonnet", "stream": true, "messages": messages})
	return data
}

func observeManagedAlignmentTurn(turn *WrapperManagedAutoTurn, text, toolID string, receipt int) {
	turn.Observe([]byte(fmt.Sprintf(`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":%q}}`, text)))
	turn.Observe([]byte(`{"type":"content_block_stop","index":0}`))
	if toolID != "" {
		turn.Observe([]byte(fmt.Sprintf(`{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":%q,"name":"lookup","input":{"item":1}}}`, toolID)))
		turn.Observe([]byte(`{"type":"content_block_stop","index":1}`))
		turn.Observe([]byte(`{"type":"message_delta","delta":{"stop_reason":"tool_use"}}`))
	} else {
		turn.Observe([]byte(`{"type":"message_delta","delta":{"stop_reason":"end_turn"}}`))
	}
	turn.Observe([]byte(fmt.Sprintf(`{"type":"message_stop","wrapper_turn_id":"00000000-0000-4000-8000-%012d"}`, receipt)))
}

func TestManagedHistorySplitAssistantToolHandoffReusesNativeSession(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	store := newWrapperManagedTestStore()
	key := WrapperManagedAutoKey(42, "split-assistant-tool")
	user := json.RawMessage(`{"role":"user","content":"lookup"}`)
	first, err := beginWrapperManagedAutoTurn(ctx, store, key)
	if err != nil {
		t.Fatal(err)
	}
	_, active, err := first.Prepare(ctx, 5, managedAlignmentRequest([]json.RawMessage{user}), managedTestContext())
	if err != nil || !active {
		t.Fatalf("first prepare: %v %v", active, err)
	}
	nativeID := first.state.NativeSessionID
	observeManagedAlignmentTurn(first, "checking", "toolu_1", 1)
	if err := first.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	first.Release()

	// The client represents one saved assistant turn as two adjacent messages.
	splitText := json.RawMessage(`{"role":"assistant","content":[{"type":"text","text":"checking"}]}`)
	splitTool := json.RawMessage(`{"role":"assistant","content":[{"type":"tool_use","id":"toolu_1","name":"lookup","input":{"item":1}}]}`)
	result := managedAlignmentToolResult("toolu_1")
	second, err := beginWrapperManagedAutoTurn(ctx, store, key)
	if err != nil {
		t.Fatal(err)
	}
	c := managedTestContext()
	wire, active, err := second.Prepare(ctx, 5, managedAlignmentRequest([]json.RawMessage{user, splitText, splitTool, result}), c)
	if err != nil || !active {
		t.Fatalf("second prepare: %v %v", active, err)
	}
	if c.GetHeader("X-Wrapper-Session-Id") != nativeID || c.GetHeader("X-Wrapper-Ack-Turn-Id") == "" ||
		gjson.GetBytes(wire, "messages.#").Int() != 1 || gjson.GetBytes(wire, "messages.0.content.0.type").String() != "tool_result" {
		t.Fatalf("tool handoff was not incremental: %s", wire)
	}
	if matched, merged, _ := second.HistoryDecision(); matched != 3 || merged != 1 {
		t.Fatalf("wrong alignment: matched=%d merged=%d", matched, merged)
	}
	observeManagedAlignmentTurn(second, "done", "", 2)
	if err := second.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	second.Release()

	third, err := beginWrapperManagedAutoTurn(ctx, store, key)
	if err != nil {
		t.Fatal(err)
	}
	defer third.Release()
	finalAssistant := json.RawMessage(`{"role":"assistant","content":[{"type":"text","text":"done"}]}`)
	newUser := json.RawMessage(`{"role":"user","content":"next"}`)
	c = managedTestContext()
	wire, active, err = third.Prepare(ctx, 5, managedAlignmentRequest([]json.RawMessage{user, splitText, splitTool, result, finalAssistant, newUser}), c)
	if err != nil || !active || c.GetHeader("X-Wrapper-Session-Id") != nativeID || gjson.GetBytes(wire, "messages.#").Int() != 1 {
		t.Fatalf("third turn did not resume: active=%v err=%v body=%s", active, err, wire)
	}
}

func TestManagedRecentMessageShapeContainsNoContent(t *testing.T) {
	messages := []json.RawMessage{
		json.RawMessage(`{"role":"assistant","content":[{"type":"text","text":"private-data"},{"type":"tool_use","id":"secret-id","name":"lookup","input":{}}]}`),
		managedAlignmentToolResult("secret-id"),
	}
	if got := managedRecentMessageShape(messages); got != "assistant:text+tool_use,user:tool_result" {
		t.Fatalf("unexpected shape: %q", got)
	}
}

func TestManagedHistorySplitAssistantRejectsChangedOrExtraContent(t *testing.T) {
	saved := []json.RawMessage{json.RawMessage(`{"role":"user","content":"lookup"}`), managedAlignmentAssistant("checking", "toolu_1")}
	cases := []struct {
		name     string
		messages []json.RawMessage
		reason   string
	}{
		{"changed_text", []json.RawMessage{saved[0], json.RawMessage(`{"role":"assistant","content":"different"}`), json.RawMessage(`{"role":"assistant","content":[{"type":"tool_use","id":"toolu_1","name":"lookup","input":{"item":1}}]}`), managedAlignmentToolResult("toolu_1")}, "history_changed"},
		{"reordered_blocks", []json.RawMessage{saved[0], json.RawMessage(`{"role":"assistant","content":[{"type":"tool_use","id":"toolu_1","name":"lookup","input":{"item":1}}]}`), json.RawMessage(`{"role":"assistant","content":"checking"}`), managedAlignmentToolResult("toolu_1")}, "history_changed"},
		{"extra_user", []json.RawMessage{saved[0], saved[1], json.RawMessage(`{"role":"user","content":"unexecuted"}`), managedAlignmentToolResult("toolu_1")}, "extra_history_messages"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, reason := matchManagedHistoryPrefix(tc.messages, saved)
			if reason != tc.reason {
				t.Fatalf("got %q, want %q", reason, tc.reason)
			}
		})
	}
}

func TestManagedHistoryFiveSplitToolRoundsKeepOneNativeSession(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	store := newWrapperManagedTestStore()
	key := WrapperManagedAutoKey(42, "five-split-tool-rounds")
	history := []json.RawMessage{json.RawMessage(`{"role":"user","content":"start"}`)}
	nativeID := ""
	for round := 1; round <= 5; round++ {
		turn, err := beginWrapperManagedAutoTurn(ctx, store, key)
		if err != nil {
			t.Fatal(err)
		}
		c := managedTestContext()
		wire, active, err := turn.Prepare(ctx, 5, managedAlignmentRequest(history), c)
		if err != nil || !active || gjson.GetBytes(wire, "messages.#").Int() != 1 {
			t.Fatalf("round %d did not send one increment: active=%v err=%v body=%s", round, active, err, wire)
		}
		if round == 1 {
			nativeID = c.GetHeader("X-Wrapper-Session-Id")
		} else if c.GetHeader("X-Wrapper-Session-Id") != nativeID || c.GetHeader("X-Wrapper-Ack-Turn-Id") == "" {
			t.Fatalf("round %d reset the native session", round)
		}
		text := fmt.Sprintf("working-%d", round)
		toolID := fmt.Sprintf("toolu_%d", round)
		observeManagedAlignmentTurn(turn, text, toolID, round)
		if err := turn.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		turn.Release()
		history = append(history,
			json.RawMessage(fmt.Sprintf(`{"role":"assistant","content":[{"type":"text","text":%q}]}`, text)),
			json.RawMessage(fmt.Sprintf(`{"role":"assistant","content":[{"type":"tool_use","id":%q,"name":"lookup","input":{"item":1}}]}`, toolID)),
			managedAlignmentToolResult(toolID),
		)
	}
}
