package repository

import (
	"context"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"testing"
	"time"
)

func TestWrapperOwnedStateRejectsExpiredWriter(t *testing.T) {
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()
	cache := &gatewayCache{rdb: client}
	ctx := context.Background()
	old, ok, err := cache.AcquireWrapperManagedLock(ctx, "audit", time.Second)
	if err != nil || !ok {
		t.Fatal(err)
	}
	mr.FastForward(2 * time.Second)
	next, ok, err := cache.AcquireWrapperManagedLock(ctx, "audit", time.Second)
	if err != nil || !ok {
		t.Fatal(err)
	}
	if ok, err = cache.SetWrapperManagedStateOwned(ctx, "audit", next, []byte("new"), time.Hour); err != nil || !ok {
		t.Fatal(err)
	}
	if ok, err = cache.SetWrapperManagedStateOwned(ctx, "audit", old, []byte("stale"), time.Hour); err != nil || ok {
		t.Fatal("stale writer accepted", err)
	}
	body, _ := cache.GetWrapperManagedState(ctx, "audit")
	if string(body) != "new" {
		t.Fatal(string(body))
	}
}
