package service

import (
	"context"
	"testing"
)

func TestWrapperJSONReplayAgentAndCapabilityIsolation(t *testing.T) {
	store := newWrapperManagedTestStore()
	body := []byte(`{"stream":false,"metadata":{"user_id":"same-parent"},"messages":[{"role":"user","content":"same"}]}`)
	response := []byte(`{"type":"message","stop_reason":"end_turn","content":[{"type":"text","text":"child-a"}]}`)
	ctx := context.Background()
	owner, _, err := beginWrapperScopedJSONReplay(ctx, store, 42, "child-a", "beta-one", body)
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.Save(ctx, response); err != nil {
		t.Fatal(err)
	}
	owner.Release()
	for _, tc := range []struct {
		key         int64
		agent, beta string
		cached      bool
	}{
		{42, "child-a", "beta-one", true}, {42, "child-b", "beta-one", false},
		{42, "", "beta-one", false}, {42, "child-a", "beta-two", false},
		{43, "child-a", "beta-one", false},
	} {
		lease, cached, err := beginWrapperScopedJSONReplay(ctx, store, tc.key, tc.agent, tc.beta, body)
		if err != nil {
			t.Fatal(err)
		}
		if lease != nil {
			lease.Release()
		}
		if (cached != nil) != tc.cached {
			t.Fatalf("wrong scope result: %+v", tc)
		}
	}
}

func TestWrapperJSONReplayUnscopedRetainsExistingResults(t *testing.T) {
	store := newWrapperManagedTestStore()
	ctx := context.Background()
	body := []byte(`{"stream":false}`)
	owner, _, err := beginWrapperJSONReplay(ctx, store, 42, body)
	if err != nil {
		t.Fatal(err)
	}
	response := []byte(`{"type":"message","stop_reason":"end_turn","content":[{"type":"text","text":"legacy"}]}`)
	if err := owner.Save(ctx, response); err != nil {
		t.Fatal(err)
	}
	owner.Release()
	lease, cached, err := beginWrapperScopedJSONReplay(ctx, store, 42, "", "", body)
	if lease != nil {
		lease.Release()
	}
	if err != nil || string(cached) != string(response) {
		t.Fatal("lost existing unscoped replay", err)
	}
}
