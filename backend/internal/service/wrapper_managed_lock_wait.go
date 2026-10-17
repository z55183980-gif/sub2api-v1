package service

import (
	"context"
	"errors"
	"os"
	"time"
)

var ErrWrapperManagedBusy = errors.New("managed session already has an active turn")

const wrapperManagedLockWait = 3 * time.Second
const wrapperManagedLockRetry = 300 * time.Millisecond

type WrapperManagedLockWait struct {
	Attempts int
	Elapsed  time.Duration
}

func beginWrapperManagedAutoTurnWithWait(ctx context.Context, store WrapperManagedStore, key string, budget time.Duration) (turn *WrapperManagedAutoTurn, stats WrapperManagedLockWait, err error) {
	start := time.Now()
	defer func() { stats.Elapsed = time.Since(start) }()
	if err = ctx.Err(); err != nil {
		return
	}
	if budget <= 0 {
		stats.Attempts = 1
		turn, err = beginWrapperManagedAutoTurn(ctx, store, key)
		return
	}
	waitCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	for {
		if err = waitCtx.Err(); err != nil {
			break
		}
		stats.Attempts++
		turn, err = beginWrapperManagedAutoTurn(waitCtx, store, key)
		if err == nil {
			return
		}
		if !errors.Is(err, ErrWrapperManagedBusy) {
			if ctx.Err() != nil {
				err = ctx.Err()
			}
			return
		}
		timer := time.NewTimer(wrapperManagedLockRetry)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, stats, ctx.Err()
		case <-waitCtx.Done():
			timer.Stop()
			err = waitCtx.Err()
		case <-timer.C:
			continue
		}
		break
	}
	if ctx.Err() != nil {
		err = ctx.Err()
	} else if errors.Is(err, context.DeadlineExceeded) && waitCtx.Err() != nil {
		err = ErrWrapperManagedBusy
	}
	return
}

func (s *GatewayService) BeginWrapperManagedAutoTurn(ctx context.Context, key string) (*WrapperManagedAutoTurn, WrapperManagedLockWait, error) {
	budget := wrapperManagedLockWait
	// Operational rollback: retain mutual exclusion, disable only waiting.
	if os.Getenv("SUB2API_WRAPPER_LOCK_WAIT_DISABLED") == "1" {
		budget = 0
	}
	turn, stats, err := beginWrapperManagedAutoTurnWithWait(ctx, s.WrapperManagedStore(), key, budget)
	if err == nil {
		deadline := time.Now().Add(budget - stats.Elapsed)
		turn.cleanupNative = func(cleanupCtx context.Context, accountID int64, id string) error {
			return retryWrapperCleanupBusy(cleanupCtx, ctx, deadline, func() error {
				return s.deleteAbandonedWrapperSession(cleanupCtx, accountID, id)
			})
		}
	}
	return turn, stats, err
}

// AccountID is read from the state loaded while holding the session lock.
func (t *WrapperManagedAutoTurn) AccountID() int64 { return t.state.AccountID }

// A canceled upstream stream may release the gateway lock just before the
// Wrapper finishes closing its CLI. Retry only its explicit busy response,
// preserving the old generation until deletion succeeds.
func retryWrapperCleanupBusy(ctx, requestCtx context.Context, deadline time.Time, cleanup func() error) error {
	for {
		err := cleanup()
		if !errors.Is(err, errWrapperCleanupBusy) {
			return err
		}
		if time.Until(deadline) < wrapperManagedLockRetry {
			return ErrWrapperManagedBusy
		}
		timer := time.NewTimer(wrapperManagedLockRetry)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-requestCtx.Done():
			timer.Stop()
			return requestCtx.Err()
		case <-timer.C:
		}
	}
}
