package service

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

func managedBudgetReminder(left int) json.RawMessage {
	return json.RawMessage(fmt.Sprintf(`{"role":"user","content":"<system_instruction>\n<total_tokens>%d tokens left</total_tokens>\n</system_instruction>"}`, left))
}

// Claude Desktop sends every tool continuation as [tool_result, reminder text].
// The whole trailing user run is one fresh turn, so the native session must be
// reused and the reminder delivered inside the live MCP result.
func TestManagedFreshUserRunKeepsNativeSessionAcrossToolRounds(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	store := newWrapperManagedTestStore()
	key := WrapperManagedAutoKey(42, "desktop-tool-rounds")
	history := []json.RawMessage{json.RawMessage(`{"role":"user","content":"start"}`)}
	nativeID := ""
	for round := 1; round <= 5; round++ {
		turn, err := beginWrapperManagedAutoTurn(ctx, store, key)
		if err != nil {
			t.Fatal(err)
		}
		c := managedTestContext()
		wire, active, err := turn.Prepare(ctx, 6, managedAlignmentRequest(history), c)
		if err != nil || !active {
			t.Fatalf("round %d: active=%v err=%v", round, active, err)
		}
		if skip, _, _ := turn.Decision(); skip != "" {
			t.Fatalf("round %d resynced: %s", round, skip)
		}
		if round == 1 {
			nativeID = c.GetHeader("X-Wrapper-Session-Id")
		} else {
			if c.GetHeader("X-Wrapper-Session-Id") != nativeID || c.GetHeader("X-Wrapper-Ack-Turn-Id") == "" {
				t.Fatalf("round %d reset the native session", round)
			}
			if fresh, _ := turn.FreshDecision(); fresh != 2 {
				t.Fatalf("round %d fresh run = %d, want 2", round, fresh)
			}
			if gjson.GetBytes(wire, "messages.#").Int() != 1 || gjson.GetBytes(wire, "messages.0.content.#").Int() != 1 {
				t.Fatalf("round %d did not send one tool-result message: %s", round, wire)
			}
			result := gjson.GetBytes(wire, "messages.0.content.0")
			if result.Get("type").String() != "tool_result" || result.Get("tool_use_id").String() != fmt.Sprintf("toolu_%d", round-1) {
				t.Fatalf("round %d wrong tool result: %s", round, result.Raw)
			}
			parts := result.Get("content").Array()
			if len(parts) != 2 || parts[0].Get("text").String() != "ok" ||
				parts[1].Get("text").String() != fmt.Sprintf("<system_instruction>\n<total_tokens>%d tokens left</total_tokens>\n</system_instruction>", 1000-round+1) {
				t.Fatalf("round %d reminder not appended to tool result: %s", round, result.Raw)
			}
		}
		toolID := fmt.Sprintf("toolu_%d", round)
		observeManagedAlignmentTurn(turn, fmt.Sprintf("working-%d", round), toolID, round)
		if err := turn.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		turn.Release()
		history = append(history, managedAlignmentAssistant(fmt.Sprintf("working-%d", round), toolID),
			managedAlignmentToolResult(toolID), managedBudgetReminder(1000-round))
	}
}

func TestManagedFreshUserRunAcceptsTextInsideToolResultMessage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	store := newWrapperManagedTestStore()
	key := WrapperManagedAutoKey(42, "desktop-inline-text")
	user := json.RawMessage(`{"role":"user","content":"start"}`)
	first, err := beginWrapperManagedAutoTurn(ctx, store, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, active, err := first.Prepare(ctx, 6, managedAlignmentRequest([]json.RawMessage{user}), managedTestContext()); err != nil || !active {
		t.Fatalf("first prepare: %v %v", active, err)
	}
	observeManagedAlignmentTurn(first, "two tools", "toolu_a", 1)
	if err := first.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	first.Release()

	second, err := beginWrapperManagedAutoTurn(ctx, store, key)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()
	results := json.RawMessage(`{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_a","content":[{"type":"text","text":"r1"}]},{"type":"text","text":"note"}]}`)
	c := managedTestContext()
	wire, active, err := second.Prepare(ctx, 6, managedAlignmentRequest([]json.RawMessage{user, managedAlignmentAssistant("two tools", "toolu_a"), results, managedBudgetReminder(5)}), c)
	if err != nil || !active {
		t.Fatalf("second prepare: %v %v", active, err)
	}
	if skip, _, _ := second.Decision(); skip != "" {
		t.Fatalf("resynced: %s", skip)
	}
	parts := gjson.GetBytes(wire, "messages.0.content.0.content").Array()
	if gjson.GetBytes(wire, "messages.0.content.#").Int() != 1 || len(parts) != 3 || parts[0].Get("text").String() != "r1" || parts[1].Get("text").String() != "note" {
		t.Fatalf("text not merged into tool result in order: %s", wire)
	}
}

func TestManagedFreshUserRunRejectsTextBeforeToolResult(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	store := newWrapperManagedTestStore()
	key := WrapperManagedAutoKey(42, "desktop-text-first")
	user := json.RawMessage(`{"role":"user","content":"start"}`)
	first, err := beginWrapperManagedAutoTurn(ctx, store, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, active, err := first.Prepare(ctx, 6, managedAlignmentRequest([]json.RawMessage{user}), managedTestContext()); err != nil || !active {
		t.Fatalf("first prepare: %v %v", active, err)
	}
	observeManagedAlignmentTurn(first, "checking", "toolu_1", 1)
	if err := first.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	first.Release()

	second, err := beginWrapperManagedAutoTurn(ctx, store, key)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()
	c := managedTestContext()
	_, _, err = second.Prepare(ctx, 6, managedAlignmentRequest([]json.RawMessage{user, managedAlignmentAssistant("checking", "toolu_1"), json.RawMessage(`{"role":"user","content":"before"}`), managedAlignmentToolResult("toolu_1")}), c)
	if err != nil {
		t.Fatal(err)
	}
	if skip, _, _ := second.Decision(); skip != "pending_tool_results_changed" {
		t.Fatalf("got skip %q", skip)
	}
	if c.GetHeader("X-Wrapper-Session-Id") == first.state.NativeSessionID {
		t.Fatal("unfaithful tool result must not resume the native session")
	}
}

func TestManagedFreshUserRunMergesPlainUserMessages(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	store := newWrapperManagedTestStore()
	key := WrapperManagedAutoKey(42, "desktop-plain-run")
	user := json.RawMessage(`{"role":"user","content":"hello"}`)
	first, err := beginWrapperManagedAutoTurn(ctx, store, key)
	if err != nil {
		t.Fatal(err)
	}
	if _, active, err := first.Prepare(ctx, 6, managedAlignmentRequest([]json.RawMessage{user}), managedTestContext()); err != nil || !active {
		t.Fatalf("first prepare: %v %v", active, err)
	}
	observeManagedAlignmentTurn(first, "hi", "", 1)
	if err := first.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	first.Release()

	assistant := json.RawMessage(`{"role":"assistant","content":[{"type":"text","text":"hi"}]}`)
	next := json.RawMessage(`{"role":"user","content":[{"type":"text","text":"question"}]}`)
	second, err := beginWrapperManagedAutoTurn(ctx, store, key)
	if err != nil {
		t.Fatal(err)
	}
	c := managedTestContext()
	wire, active, err := second.Prepare(ctx, 6, managedAlignmentRequest([]json.RawMessage{user, assistant, next, managedBudgetReminder(9)}), c)
	if err != nil || !active || c.GetHeader("X-Wrapper-Session-Id") != first.state.NativeSessionID {
		t.Fatalf("plain run did not resume: active=%v err=%v", active, err)
	}
	if gjson.GetBytes(wire, "messages.#").Int() != 1 || gjson.GetBytes(wire, "messages.0.content.#").Int() != 2 ||
		gjson.GetBytes(wire, "messages.0.content.0.text").String() != "question" {
		t.Fatalf("plain run not merged: %s", wire)
	}
	observeManagedAlignmentTurn(second, "answer", "", 2)
	if err := second.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	second.Release()

	// Both fresh messages are saved as history, so the next request matches.
	third, err := beginWrapperManagedAutoTurn(ctx, store, key)
	if err != nil {
		t.Fatal(err)
	}
	defer third.Release()
	c = managedTestContext()
	_, active, err = third.Prepare(ctx, 6, managedAlignmentRequest([]json.RawMessage{user, assistant, next, managedBudgetReminder(9),
		json.RawMessage(`{"role":"assistant","content":[{"type":"text","text":"answer"}]}`), json.RawMessage(`{"role":"user","content":"more"}`)}), c)
	if err != nil || !active || c.GetHeader("X-Wrapper-Session-Id") != first.state.NativeSessionID {
		t.Fatalf("third turn did not resume: active=%v err=%v", active, err)
	}
	if skip, _, _ := third.Decision(); skip != "" {
		t.Fatalf("third turn resynced: %s", skip)
	}
}
