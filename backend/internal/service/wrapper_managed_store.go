package service

import (
	"context"
	"time"
)

// WrapperManagedStore is deliberately separate from GatewayCache so existing
// cache implementations do not silently acquire a session-resume contract.
type WrapperManagedStore interface {
	GetWrapperManagedState(ctx context.Context, key string) ([]byte, error)
	SetWrapperManagedState(ctx context.Context, key string, value []byte, ttl time.Duration) error
	AcquireWrapperManagedLock(ctx context.Context, key string, ttl time.Duration) (string, bool, error)
	RefreshWrapperManagedLock(ctx context.Context, key, token string, ttl time.Duration) (bool, error)
	ReleaseWrapperManagedLock(ctx context.Context, key, token string) error
}
