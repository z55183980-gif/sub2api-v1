package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sync"
	"time"
)

// JSON retries share a single execution. Only a complete validated response is
// retained; SSE fragments, auth headers and failed responses are never replayed.
type WrapperJSONReplay struct {
	store      WrapperManagedStore
	key, token string
	stop, done chan struct{}
	once       sync.Once
	mu         sync.Mutex
	lost       bool
	leaseLost  chan struct{}
}

func (s *GatewayService) BeginWrapperJSONReplay(ctx context.Context, keyID int64, body []byte) (*WrapperJSONReplay, []byte, error) {
	return beginWrapperJSONReplay(ctx, s.WrapperManagedStore(), keyID, body)
}

// Agent identity and beta capabilities live in headers, outside the JSON body.
// Different agents must never retrieve one another's completed response.
func (s *GatewayService) BeginWrapperScopedJSONReplay(ctx context.Context, keyID int64, agentID, beta string, body []byte) (*WrapperJSONReplay, []byte, error) {
	return beginWrapperScopedJSONReplay(ctx, s.WrapperManagedStore(), keyID, agentID, beta, body)
}

func beginWrapperScopedJSONReplay(ctx context.Context, store WrapperManagedStore, keyID int64, agentID, beta string, body []byte) (*WrapperJSONReplay, []byte, error) {
	if agentID == "" && beta == "" {
		return beginWrapperJSONReplay(ctx, store, keyID, body)
	}
	if !json.Valid(body) {
		return nil, nil, errors.New("invalid replay request JSON")
	}
	// An array envelope cannot collide with a valid Messages request object.
	scoped, err := json.Marshal([]any{"wrapper-json-scope-v2", agentID, beta, json.RawMessage(body)})
	if err != nil {
		return nil, nil, err
	}
	return beginWrapperJSONReplay(ctx, store, keyID, scoped)
}
func beginWrapperJSONReplay(ctx context.Context, store WrapperManagedStore, keyID int64, body []byte) (*WrapperJSONReplay, []byte, error) {
	var value any
	if err := json.Unmarshal(body, &value); err != nil {
		return nil, nil, err
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return nil, nil, err
	}
	h := sha256.Sum256(canonical)
	key := "json-replay-v1:" + WrapperManagedAutoKey(keyID, hex.EncodeToString(h[:]))
	for {
		if err = ctx.Err(); err != nil {
			return nil, nil, err
		}
		cached, e := store.GetWrapperManagedState(ctx, key)
		if e != nil {
			return nil, nil, e
		}
		if len(cached) > 0 {
			return nil, cached, nil
		}
		token, ok, e := store.AcquireWrapperManagedLock(ctx, key, 45*time.Second)
		if e != nil {
			return nil, nil, e
		}
		if ok {
			lease := &WrapperJSONReplay{store: store, key: key, token: token, stop: make(chan struct{}), done: make(chan struct{}), leaseLost: make(chan struct{})}
			go lease.renew()
			// Recheck after acquiring: previous owner may have completed between
			// the initial read and acquisition.
			cached, e = store.GetWrapperManagedState(ctx, key)
			if e != nil || len(cached) > 0 {
				lease.Release()
				return nil, cached, e
			}
			return lease, nil, nil
		}
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
}
func (l *WrapperJSONReplay) renew() {
	defer close(l.done)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-l.stop:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			ok, e := l.store.RefreshWrapperManagedLock(ctx, l.key, l.token, 45*time.Second)
			cancel()
			if e != nil || !ok {
				l.mu.Lock()
				l.lost = true
				l.mu.Unlock()
				close(l.leaseLost)
				return
			}
		}
	}
}
func (l *WrapperJSONReplay) Save(ctx context.Context, body []byte) error {
	l.mu.Lock()
	lost := l.lost
	l.mu.Unlock()
	if lost {
		return errors.New("JSON execution lease lost")
	}
	// Never persist an error or partially assembled model message.
	var m struct {
		Type       string          `json:"type"`
		StopReason string          `json:"stop_reason"`
		Content    json.RawMessage `json:"content"`
	}
	if len(body) > 32<<20 || json.Unmarshal(body, &m) != nil || m.Type != "message" || m.StopReason == "" || len(m.Content) == 0 {
		return errors.New("JSON response is not a complete message")
	}
	ok, e := l.store.RefreshWrapperManagedLock(ctx, l.key, l.token, 45*time.Second)
	if e != nil {
		return e
	}
	if !ok {
		return errors.New("JSON execution lease lost")
	}
	ok, e = l.store.SetWrapperManagedStateOwned(ctx, l.key, l.token, body, 10*time.Minute)
	if e != nil {
		return e
	}
	if !ok {
		return errors.New("JSON execution lease lost")
	}
	return nil
}
func (l *WrapperJSONReplay) GuardContext(parent context.Context) context.Context {
	return guardWrapperLease(parent, l.leaseLost, l.stop)
}
func (l *WrapperJSONReplay) Release() {
	l.once.Do(func() {
		close(l.stop)
		<-l.done
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = l.store.ReleaseWrapperManagedLock(ctx, l.key, l.token)
	})
}
