package service

import (
	"context"
	"testing"
)

func TestWrapperManagedConversationIdentity(t *testing.T) {
	parent := "f804bbdc-8197-469d-8c4d-542cde48203d"
	if got := WrapperManagedConversationID(parent, ""); got != parent {
		t.Fatal("main session migration would lose its pending tool handoff")
	}
	a := WrapperManagedConversationID(parent, "ae46f70080fa89993")
	b := WrapperManagedConversationID(parent, "a61db7c07ad1c5b16")
	if a == b || a == parent || a != WrapperManagedConversationID(parent, "ae46f70080fa89993") {
		t.Fatal("subagents must be isolated and retries stable")
	}
	if a == WrapperManagedConversationID("another-session", "ae46f70080fa89993") {
		t.Fatal("agent identity must remain scoped to its parent session")
	}
	if WrapperManagedConversationID("a:b", "c") == WrapperManagedConversationID("a", "b:c") {
		t.Fatal("ambiguous identity framing")
	}
	if WrapperManagedAutoKey(1, a) == WrapperManagedAutoKey(2, a) {
		t.Fatal("API key isolation lost")
	}
}

func TestWrapperManagedAgentLocksAndPendingStateAreIndependent(t *testing.T) {
	ctx := context.Background()
	store := newWrapperManagedTestStore()
	key := func(agent string) string {
		return WrapperManagedAutoKey(1, WrapperManagedConversationID("parent", agent))
	}
	parent, err := beginWrapperManagedAutoTurn(ctx, store, key(""))
	if err != nil {
		t.Fatal(err)
	}
	defer parent.Release()
	parent.state = wrapperManagedState{AccountID: 5, NativeSessionID: "native-parent", Phase: "ready", Receipt: "receipt-parent", PendingToolIDs: []string{"toolu_parent"}}
	if err := parent.save(ctx); err != nil {
		t.Fatal(err)
	}
	before, _ := store.GetWrapperManagedState(ctx, key(""))
	for _, agent := range []string{"agent-a", "agent-b"} {
		child, err := beginWrapperManagedAutoTurn(ctx, store, key(agent))
		if err != nil {
			t.Fatalf("parent lock blocks child: %v", err)
		}
		defer child.Release()
		if child.state.NativeSessionID != "" || len(child.state.PendingToolIDs) != 0 {
			t.Fatal("child inherited parent state")
		}
		child.state = wrapperManagedState{AccountID: 8, NativeSessionID: "native-" + agent, Phase: "ready"}
		if err := child.save(ctx); err != nil {
			t.Fatal(err)
		}
		if retry, err := beginWrapperManagedAutoTurn(ctx, store, key(agent)); err == nil {
			retry.Release()
			t.Fatal("same agent bypassed its active lock")
		}
	}
	after, _ := store.GetWrapperManagedState(ctx, key(""))
	if string(before) != string(after) {
		t.Fatal("child overwrote parent pending tool result")
	}
}
