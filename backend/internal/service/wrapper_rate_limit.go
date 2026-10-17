package service

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/tidwall/gjson"
)

// Shared by account snapshots in this gateway process, closing the interval
// between observing a quota error and the database/outbox snapshot refresh.
var wrapperQuotaBlocks sync.Map

func wrapperQuotaBlocked(accountID int64) bool {
	value, ok := wrapperQuotaBlocks.Load(accountID)
	if !ok {
		return false
	}
	until := value.(time.Time)
	if time.Now().Before(until) {
		return true
	}
	wrapperQuotaBlocks.CompareAndDelete(accountID, value)
	return false
}

func blockWrapperQuota(accountID int64, until time.Time) {
	wrapperQuotaBlocks.Range(func(key, value any) bool {
		if !time.Now().Before(value.(time.Time)) {
			wrapperQuotaBlocks.CompareAndDelete(key, value)
		}
		return true
	})
	for {
		previous, loaded := wrapperQuotaBlocks.LoadOrStore(accountID, until)
		if !loaded || !until.After(previous.(time.Time)) {
			return
		}
		if wrapperQuotaBlocks.CompareAndSwap(accountID, previous, until) {
			return
		}
	}
}

// Wrapper errors use the same metadata before and after HTTP headers are sent.
func (s *RateLimitService) handleWrapperRateLimit(ctx context.Context, account *Account, headers http.Header, body []byte) {
	if s == nil || account == nil || s.accountRepo == nil {
		return
	}
	now := time.Now()
	reset := gjson.GetBytes(body, "error.reset_at").String()
	if reset == "" {
		reset = headers.Get("anthropic-ratelimit-unified-reset")
	}
	seconds, _ := strconv.ParseInt(reset, 10, 64)
	until := time.Unix(seconds, 0)
	fallback := !until.After(now) || until.After(now.Add(8*24*time.Hour))
	if fallback {
		// Persist the strike counter so gateway restarts do not reset backoff.
		data, _ := json.Marshal(account.Extra)
		strikes := max(0, min(6, int(gjson.GetBytes(data, "wrapper_limit_strikes").Int())))
		last := gjson.GetBytes(data, "wrapper_limit_last_at").Int()
		if now.Unix()-last > 3600 {
			strikes = 0
		}
		if strikes < 6 {
			strikes++
		}
		until = now.Add(time.Duration(30*(1<<max(0, strikes-1))) * time.Second)
		if err := s.accountRepo.UpdateExtra(ctx, account.ID, map[string]any{"wrapper_limit_strikes": strikes, "wrapper_limit_last_at": now.Unix()}); err != nil {
			slog.Warn("wrapper_quota_backoff_persist_failed", "account_id", account.ID, "error", err)
		}
	}
	if account.RateLimitResetAt != nil && account.RateLimitResetAt.After(until) {
		until = *account.RateLimitResetAt
	}
	s.notifyAccountSchedulingBlocked(account, until, "wrapper_quota_exhausted")
	blockWrapperQuota(account.ID, until)
	var err error
	if repo, ok := s.accountRepo.(interface {
		SetRateLimitedIfLater(context.Context, int64, time.Time) error
	}); ok {
		err = repo.SetRateLimitedIfLater(ctx, account.ID, until)
	} else {
		err = s.accountRepo.SetRateLimited(ctx, account.ID, until)
	}
	if err != nil {
		slog.Warn("wrapper_quota_cooldown_persist_failed", "account_id", account.ID, "error", err)
	}
	slog.Info("wrapper_quota_cooldown", "account_id", account.ID, "reset_at", until, "fallback", fallback)
}
