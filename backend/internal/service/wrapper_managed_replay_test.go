package service

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestWrapperJSONReplayJoinsAndCachesCompleteResponse(t *testing.T) {
	store := newWrapperManagedTestStore()
	ctx := context.Background()
	body := []byte(`{"stream":false,"messages":[{"role":"user","content":"test"}]}`)
	owner, cached, e := beginWrapperJSONReplay(ctx, store, 42, body)
	if e != nil || cached != nil || owner == nil {
		t.Fatal(e)
	}
	defer owner.Release()
	done := make(chan []byte, 1)
	errs := make(chan error, 1)
	go func() {
		wait, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		lease, data, e := beginWrapperJSONReplay(wait, store, 42, body)
		if lease != nil {
			lease.Release()
			t.Error("duplicate executed")
		}
		errs <- e
		done <- data
	}()
	select {
	case <-done:
		t.Fatal("returned before original completed")
	case <-time.After(40 * time.Millisecond):
	}
	response := []byte(`{"type":"message","stop_reason":"end_turn","content":[{"type":"text","text":"complete"}]}`)
	if e = owner.Save(ctx, response); e != nil {
		t.Fatal(e)
	}
	owner.Release()
	if e = <-errs; e != nil {
		t.Fatal(e)
	}
	if !bytes.Equal(<-done, response) {
		t.Fatal("different response")
	}
	lease, data, e := beginWrapperJSONReplay(ctx, store, 42, body)
	if e != nil || lease != nil || !bytes.Equal(data, response) {
		t.Fatal("completed retry executed", e)
	}
	other, data, e := beginWrapperJSONReplay(ctx, store, 43, body)
	if e != nil || other == nil || data != nil {
		t.Fatal("cross key reuse", e)
	}
	other.Release()
}
func TestWrapperJSONReplayCancellationAndInvalidResponse(t *testing.T) {
	store := newWrapperManagedTestStore()
	ctx := context.Background()
	body := []byte(`{"stream":false}`)
	owner, _, e := beginWrapperJSONReplay(ctx, store, 42, body)
	if e != nil {
		t.Fatal(e)
	}
	defer owner.Release()
	if e = owner.Save(ctx, []byte(`{"type":"error","error":{}}`)); e == nil {
		t.Fatal("cached error")
	}
	wait, cancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer cancel()
	if _, _, e = beginWrapperJSONReplay(wait, store, 42, body); e == nil {
		t.Fatal("wait ignored cancellation")
	}
	if len(store.states) != 0 {
		t.Fatal("saved partial response")
	}
}
