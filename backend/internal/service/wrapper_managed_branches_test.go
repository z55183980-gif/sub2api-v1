package service

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"github.com/tidwall/gjson"
)

const branchUA = "claude-cli/2.1.293 (external, claude-desktop-3p)"

func branchBody(messages []json.RawMessage) []byte { return managedAlignmentRequest(messages) }

func TestManagedBranchesParallelAndToolContinuation(t *testing.T) {
	ctx := context.Background()
	store := newWrapperManagedTestStore()
	ids := map[string]bool{}
	for i := 0; i < 5; i++ {
		user := json.RawMessage(fmt.Sprintf(`{"role":"user","content":"task %d"}`, i))
		body := branchBody([]json.RawMessage{user})
		id, source, err := resolveWrapperManagedBranch(ctx, store, 42, "parent", body, "", branchUA)
		if err != nil || source != "new_history" || ids[id] {
			t.Fatalf("isolation %s %s %v", id, source, err)
		}
		ids[id] = true
		turn, err := beginWrapperManagedAutoTurn(ctx, store, WrapperManagedAutoKey(42, id))
		if err != nil {
			t.Fatal(err)
		}
		if _, active, e := turn.Prepare(ctx, 8, body, managedTestContext()); e != nil || !active {
			t.Fatalf("prepare %v", e)
		}
		retry, _, e := resolveWrapperManagedBranch(ctx, store, 42, "parent", body, "", branchUA)
		if e != nil || retry != id {
			t.Fatal("pending duplicate split", e)
		}
		if other, e := beginWrapperManagedAutoTurn(ctx, store, WrapperManagedAutoKey(42, retry)); e == nil {
			other.Release()
			t.Fatal("duplicate bypassed lock")
		}
		tool := json.RawMessage(fmt.Sprintf(`{"type":"tool_use","id":"tool-%d","name":"Read","input":{"file_path":"probe.txt"}}`, i))
		turn.blocks[0] = tool
		turn.terminal = true
		turn.stopReason = "tool_use"
		turn.receipt = fmt.Sprintf("receipt-%d", i)
		if err = turn.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		native := turn.state.NativeSessionID
		turn.Release()
		assistant := json.RawMessage(`{"role":"assistant","content":[` + string(tool) + `]}`)
		result := json.RawMessage(fmt.Sprintf(`{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool-%d","content":"real result"}]}`, i))
		nextBody := branchBody([]json.RawMessage{user, assistant, result})
		nextID, source, e := resolveWrapperManagedBranch(ctx, store, 42, "parent", nextBody, "", branchUA)
		if e != nil || nextID != id || source != "history" {
			t.Fatalf("continuation %s %v", source, e)
		}
		next, e := beginWrapperManagedAutoTurn(ctx, store, WrapperManagedAutoKey(42, nextID))
		if e != nil {
			t.Fatal(e)
		}
		c := managedTestContext()
		wire, active, e := next.Prepare(ctx, 8, nextBody, c)
		if e != nil || !active || next.state.NativeSessionID != native || c.GetHeader("X-Wrapper-Ack-Turn-Id") == "" || gjson.GetBytes(wire, "messages.#").Int() != 1 {
			t.Fatalf("native reuse %v", e)
		}
		retry, _, e = resolveWrapperManagedBranch(ctx, store, 42, "parent", nextBody, "", branchUA)
		if e != nil || retry != id {
			t.Fatal("continuation retry split", e)
		}
		// Keep generation active while creating the next unrelated branch.
		defer next.Release()
	}
}

func TestManagedBranchesConcurrentFirstRequest(t *testing.T) {
	store := newWrapperManagedTestStore()
	body := branchBody([]json.RawMessage{json.RawMessage(`{"role":"user","content":"same"}`)})
	var wg sync.WaitGroup
	ids := make(chan string, 12)
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, _, e := resolveWrapperManagedBranch(context.Background(), store, 42, "parent", body, "", branchUA)
			if e != nil {
				t.Error(e)
			}
			ids <- id
		}()
	}
	wg.Wait()
	close(ids)
	first := ""
	for id := range ids {
		if first == "" {
			first = id
		}
		if id != first {
			t.Fatal("duplicate allocated distinct branches")
		}
	}
	other, _, e := resolveWrapperManagedBranch(context.Background(), store, 43, "parent", body, "", branchUA)
	if e != nil {
		t.Fatal(e)
	}
	if WrapperManagedAutoKey(43, other) == WrapperManagedAutoKey(42, first) {
		t.Fatal("cross key collision")
	}
}

func TestManagedBranchesLegacyBashMigrationAndUnknownResult(t *testing.T) {
	ctx := context.Background()
	store := newWrapperManagedTestStore()
	user := json.RawMessage(`{"role":"user","content":"run diagnostic"}`)
	original := bashReconcileMessage("cd /tmp/test && pwd")
	returned := bashReconcileMessage("pwd")
	result := json.RawMessage(`{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool-bash","content":"/tmp/test"}]}`)
	var model struct {
		Model string `json:"model"`
	}
	_ = json.Unmarshal(branchBody(nil), &model)
	state := wrapperManagedState{AccountID: 8, Model: model.Model, Phase: "ready", Receipt: "old-receipt", Messages: []json.RawMessage{user, original}, PendingToolIDs: []string{"tool-bash"}}
	data, _ := json.Marshal(state)
	key := WrapperManagedAutoKey(42, "parent")
	_ = store.SetWrapperManagedState(ctx, key, data, wrapperManagedTTL)
	id, source, e := resolveWrapperManagedBranch(ctx, store, 42, "parent", branchBody([]json.RawMessage{user, returned, result}), "", branchUA)
	if e != nil || id != "parent" || source != "history" {
		t.Fatalf("legacy migration %s %s %v", id, source, e)
	}
	after, _ := store.GetWrapperManagedState(ctx, key)
	if string(after) != string(data) {
		t.Fatal("resolver mutated live state")
	}
	bad := json.RawMessage(`{"role":"user","content":"different task"}`)
	_, _, e = resolveWrapperManagedBranch(ctx, store, 42, "parent", branchBody([]json.RawMessage{bad, result}), "", branchUA)
	if e == nil {
		t.Fatal("unowned tool result accepted")
	}
	after, _ = store.GetWrapperManagedState(ctx, key)
	if string(after) != string(data) {
		t.Fatal("unknown branch destroyed old handoff")
	}
}

func TestManagedBranchStaleResolutionCannotDeleteAdvancedState(t *testing.T) {
	ctx := context.Background()
	store := newWrapperManagedTestStore()
	key := WrapperManagedAutoKey(42, "branch")
	turn, e := beginWrapperManagedAutoTurn(ctx, store, key)
	if e != nil {
		t.Fatal(e)
	}
	defer turn.Release()
	user := json.RawMessage(`{"role":"user","content":"original"}`)
	turn.state = wrapperManagedState{AccountID: 8, Model: "sonnet", Phase: "ready", Receipt: "r", NativeSessionID: "keep-native", Messages: []json.RawMessage{user, json.RawMessage(`{"role":"assistant","content":"already advanced"}`)}}
	if e = turn.save(ctx); e != nil {
		t.Fatal(e)
	}
	before, _ := store.GetWrapperManagedState(ctx, key)
	deleted := false
	turn.cleanupNative = func(context.Context, int64, string) error { deleted = true; return nil }
	turn.PreserveBranch()
	_, _, e = turn.Prepare(ctx, 8, branchBody([]json.RawMessage{user}), managedTestContext())
	if e == nil || deleted || turn.state.NativeSessionID != "keep-native" {
		t.Fatalf("stale snapshot modified state: %v", e)
	}
	after, _ := store.GetWrapperManagedState(ctx, key)
	if string(before) != string(after) {
		t.Fatal("stale request changed Redis state")
	}
}

func TestManagedBranchClosedToolHistoryRecovery(t *testing.T) {
	ctx := context.Background()
	store := newWrapperManagedTestStore()
	user := json.RawMessage(`{"role":"user","content":"read the marker"}`)
	assistant := json.RawMessage(`{"role":"assistant","content":[{"type":"tool_use","id":"lost-tool","name":"Read","input":{"file_path":"probe.txt"}}]}`)
	result := json.RawMessage(`{"role":"user","content":[{"type":"tool_result","tool_use_id":"lost-tool","content":"ACTUAL_MARKER"}]}`)
	body := branchBody([]json.RawMessage{user, assistant, result})
	id, _, e := resolveWrapperManagedBranch(ctx, store, 42, "parent", body, "", branchUA)
	if e != nil {
		t.Fatal(e)
	}
	turn, e := beginWrapperManagedAutoTurn(ctx, store, WrapperManagedAutoKey(42, id))
	if e != nil {
		t.Fatal(e)
	}
	turn.PreserveBranch()
	wire, active, e := turn.Prepare(ctx, 8, body, managedTestContext())
	if e != nil || !active || turn.skipReason != "branch_complete_tool_history_recovered" || gjson.GetBytes(wire, "messages.#").Int() != 4 {
		t.Fatalf("recover %v %s", e, turn.skipReason)
	}
	turn.blocks[0] = json.RawMessage(`{"type":"text","text":"marker accepted"}`)
	turn.terminal = true
	turn.stopReason = "end_turn"
	turn.receipt = "recovered-receipt"
	if e = turn.Commit(ctx); e != nil {
		t.Fatal(e)
	}
	native := turn.state.NativeSessionID
	turn.Release()
	next := branchBody([]json.RawMessage{user, assistant, result, json.RawMessage(`{"role":"assistant","content":"marker accepted"}`), json.RawMessage(`{"role":"user","content":"next"}`)})
	nextID, source, e := resolveWrapperManagedBranch(ctx, store, 42, "parent", next, "", branchUA)
	if e != nil || nextID != id || source != "history" {
		t.Fatalf("ledger polluted: %v", e)
	}
	two, e := beginWrapperManagedAutoTurn(ctx, store, WrapperManagedAutoKey(42, id))
	if e != nil {
		t.Fatal(e)
	}
	defer two.Release()
	two.PreserveBranch()
	c := managedTestContext()
	_, active, e = two.Prepare(ctx, 8, next, c)
	if e != nil || !active || two.state.NativeSessionID != native || c.GetHeader("X-Wrapper-Ack-Turn-Id") == "" {
		t.Fatalf("recovery not reusable %v", e)
	}
}
