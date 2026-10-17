package service

import (
	"context"
	"testing"
	"time"
)

func TestWrapperLostOwnerCannotOverwriteOrQuarantineNewState(t *testing.T) {
	store := newWrapperManagedTestStore()
	turn, err := beginWrapperManagedAutoTurn(context.Background(), store, "fenced")
	if err != nil {
		t.Fatal(err)
	}
	defer turn.Release()
	store.mu.Lock()
	store.locks["fenced"] = "new-owner"
	store.states["fenced"] = []byte(`{"phase":"ready","receipt":"new"}`)
	store.mu.Unlock()
	if err := turn.save(context.Background()); err == nil {
		t.Fatal("old owner wrote state")
	}
	turn.Quarantine()
	state, _ := store.GetWrapperManagedState(context.Background(), "fenced")
	if string(state) != `{"phase":"ready","receipt":"new"}` {
		t.Fatalf("new owner state overwritten: %s", state)
	}
}

func TestWrapperLeaseLossCancelsExecutionButReleaseDoesNot(t *testing.T) {
	for _, lose := range []bool{false, true} {
		parent, cancel := context.WithCancel(context.Background())
		lost, stop := make(chan struct{}), make(chan struct{})
		ctx := guardWrapperLease(parent, lost, stop)
		if lose {
			close(lost)
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
				t.Fatal("execution not canceled")
			}
		} else {
			close(stop)
			select {
			case <-ctx.Done():
				t.Fatal("normal release canceled request")
			case <-time.After(10 * time.Millisecond):
			}
		}
		cancel()
	}
}

func TestWrapperJSONReplayLostOwnerCannotSave(t *testing.T) {
	store := newWrapperManagedTestStore()
	lease, _, err := beginWrapperJSONReplay(context.Background(), store, 1, []byte(`{"messages":[]}`))
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	store.mu.Lock()
	store.locks[lease.key] = "new-owner"
	store.states[lease.key] = []byte("new-result")
	store.mu.Unlock()
	if err := lease.Save(context.Background(), []byte(`{"type":"message","stop_reason":"end_turn","content":[]}`)); err == nil {
		t.Fatal("stale replay write accepted")
	}
	state, _ := store.GetWrapperManagedState(context.Background(), lease.key)
	if string(state) != "new-result" {
		t.Fatal("new result overwritten")
	}
}
