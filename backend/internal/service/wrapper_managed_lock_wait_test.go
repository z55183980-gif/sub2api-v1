package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type timedManagedStore struct {
	*wrapperManagedTestStore
	muTimes sync.Mutex
	times   []time.Time
	err     error
}

func (s *timedManagedStore) AcquireWrapperManagedLock(ctx context.Context, key string, ttl time.Duration) (string, bool, error) {
	s.muTimes.Lock()
	s.times = append(s.times, time.Now())
	s.muTimes.Unlock()
	if s.err != nil {
		return "", false, s.err
	}
	return s.wrapperManagedTestStore.AcquireWrapperManagedLock(ctx, key, ttl)
}

func TestWrapperManagedLockWaitReadsLatestState(t *testing.T) {
	s := &timedManagedStore{wrapperManagedTestStore: newWrapperManagedTestStore()}
	first, err := beginWrapperManagedAutoTurn(context.Background(), s, "same")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()
	done := make(chan struct{})
	go func() {
		defer close(done)
		time.Sleep(450 * time.Millisecond)
		_ = s.SetWrapperManagedState(context.Background(), "same", []byte(`{"account_id":8,"phase":"ready","receipt":"latest"}`), time.Hour)
		first.Release()
	}()
	turn, stats, err := beginWrapperManagedAutoTurnWithWait(context.Background(), s, "same", wrapperManagedLockWait)
	<-done
	if err != nil {
		t.Fatal(err)
	}
	defer turn.Release()
	if turn.AccountID() != 8 || turn.state.Receipt != "latest" || stats.Attempts < 3 {
		t.Fatalf("stale state or no retry: %+v %+v", turn.state, stats)
	}
	s.muTimes.Lock()
	defer s.muTimes.Unlock()
	for i := 2; i < len(s.times); i++ {
		if s.times[i].Sub(s.times[i-1]) < 290*time.Millisecond {
			t.Fatal("retry interval too short")
		}
	}
}

func TestWrapperManagedLockWaitTimeoutPreservesOwner(t *testing.T) {
	s := newWrapperManagedTestStore()
	first, err := beginWrapperManagedAutoTurn(context.Background(), s, "same")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()
	turn, stats, err := beginWrapperManagedAutoTurnWithWait(context.Background(), s, "same", wrapperManagedLockWait)
	if turn != nil || !errors.Is(err, ErrWrapperManagedBusy) || stats.Elapsed < 2900*time.Millisecond || stats.Elapsed > 4*time.Second {
		t.Fatalf("timeout: %+v %v", stats, err)
	}
	if ok, _ := s.RefreshWrapperManagedLock(context.Background(), "same", first.token, time.Minute); !ok {
		t.Fatal("waiting request stole or deleted owner lock")
	}
	other, _, err := beginWrapperManagedAutoTurnWithWait(context.Background(), s, "different", wrapperManagedLockWait)
	if err != nil {
		t.Fatal(err)
	}
	other.Release()
}

func TestWrapperManagedLockWaitCancellationAndStoreError(t *testing.T) {
	s := newWrapperManagedTestStore()
	first, err := beginWrapperManagedAutoTurn(context.Background(), s, "same")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	time.AfterFunc(50*time.Millisecond, cancel)
	turn, stats, err := beginWrapperManagedAutoTurnWithWait(ctx, s, "same", wrapperManagedLockWait)
	if turn != nil || !errors.Is(err, context.Canceled) || stats.Elapsed > 250*time.Millisecond {
		t.Fatalf("cancellation: %+v %v", stats, err)
	}
	storeErr := errors.New("redis unavailable")
	broken := &timedManagedStore{wrapperManagedTestStore: s, err: storeErr}
	_, stats, err = beginWrapperManagedAutoTurnWithWait(context.Background(), broken, "same", wrapperManagedLockWait)
	if !errors.Is(err, storeErr) || stats.Attempts != 1 {
		t.Fatalf("storage error was retried: %+v %v", stats, err)
	}
	_, stats, err = beginWrapperManagedAutoTurnWithWait(context.Background(), s, "same", 0)
	if !errors.Is(err, ErrWrapperManagedBusy) || stats.Attempts != 1 {
		t.Fatal("disabled waiting must retain exclusion")
	}
}

func TestWrapperManagedLockWaitSerializesWaiters(t *testing.T) {
	s := newWrapperManagedTestStore()
	first, err := beginWrapperManagedAutoTurn(context.Background(), s, "same")
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()
	var wg sync.WaitGroup
	var mu sync.Mutex
	active := 0
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			turn, _, err := beginWrapperManagedAutoTurnWithWait(context.Background(), s, "same", wrapperManagedLockWait)
			if err != nil {
				t.Error(err)
				return
			}
			defer turn.Release()
			mu.Lock()
			active++
			if active != 1 {
				t.Error("simultaneous owners")
			}
			mu.Unlock()
			time.Sleep(50 * time.Millisecond)
			mu.Lock()
			active--
			mu.Unlock()
		}()
	}
	time.Sleep(100 * time.Millisecond)
	first.Release()
	wg.Wait()
}

func TestWrapperManagedCleanupBusyWait(t *testing.T) {
	calls := 0
	err := retryWrapperCleanupBusy(context.Background(), context.Background(), time.Now().Add(time.Second), func() error {
		calls++
		if calls < 3 {
			return errWrapperCleanupBusy
		}
		return nil
	})
	if err != nil || calls != 3 {
		t.Fatalf("cleanup did not retry: %d %v", calls, err)
	}
	calls = 0
	err = retryWrapperCleanupBusy(context.Background(), context.Background(), time.Now().Add(100*time.Millisecond), func() error { calls++; return errWrapperCleanupBusy })
	if !errors.Is(err, ErrWrapperManagedBusy) || calls != 1 {
		t.Fatal("cleanup exceeded remaining budget")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err = retryWrapperCleanupBusy(context.Background(), ctx, time.Now().Add(time.Second), func() error { return errWrapperCleanupBusy })
	if !errors.Is(err, context.Canceled) {
		t.Fatal("cleanup ignored request cancellation")
	}
	storeErr := errors.New("transport failed")
	err = retryWrapperCleanupBusy(context.Background(), context.Background(), time.Now().Add(time.Second), func() error { return storeErr })
	if !errors.Is(err, storeErr) {
		t.Fatal("retried non-busy error")
	}
}
