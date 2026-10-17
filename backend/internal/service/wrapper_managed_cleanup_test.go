package service

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/stretchr/testify/require"
)

type liveCleanupUpstream struct{ HTTPUpstream }

func (u *liveCleanupUpstream) DoWithTLS(req *http.Request, _ string, _ int64, _ int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return (&http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}).Do(req)
}

// Opt-in integration check against the isolated, loopback-only candidate.
func TestWrapperManagedCleanupLive(t *testing.T) {
	id := os.Getenv("WRAPPER_CLEANUP_TEST_ID")
	if id == "" {
		t.Skip("isolated live candidate not requested")
	}
	endpoint := os.Getenv("WRAPPER_CLEANUP_TEST_URL")
	require.True(t, strings.HasPrefix(id, "sub2api-probe-"))
	require.Equal(t, "http://127.0.0.1:24699", endpoint)
	account := newAnthropicAPIKeyAccountForTest()
	account.Credentials["base_url"] = endpoint
	account.Credentials["api_key"] = "isolated-probe"
	account.Extra = map[string]any{"wrapper_protocol": "anthropic-messages-cli-v1"}
	svc := &GatewayService{accountRepo: &cleanupAccountRepo{account: account}, httpUpstream: &liveCleanupUpstream{}, cfg: &config.Config{Security: config.SecurityConfig{URLAllowlist: config.URLAllowlistConfig{AllowInsecureHTTP: true}}}}
	turn, err := beginWrapperManagedAutoTurn(context.Background(), newWrapperManagedTestStore(), WrapperManagedAutoKey(1, "live-cleanup"))
	require.NoError(t, err)
	defer turn.Release()
	turn.cleanupNative = svc.deleteAbandonedWrapperSession
	turn.state = wrapperManagedState{AccountID: account.ID, NativeSessionID: id, Phase: "ready", PendingToolIDs: []string{"toolu_waiting"}}
	require.NoError(t, turn.abandonNativeSession(context.Background(), "account_changed"))
	require.Empty(t, turn.state.NativeSessionID)
}

type cleanupAccountRepo struct {
	AccountRepository
	account *Account
}

func (r *cleanupAccountRepo) GetByID(_ context.Context, id int64) (*Account, error) {
	if id != r.account.ID {
		return nil, errors.New("unexpected account")
	}
	return r.account, nil
}

func TestWrapperManagedCleanupUsesOldAccountCredentialsAndExactGeneration(t *testing.T) {
	account := newAnthropicAPIKeyAccountForTest()
	account.Extra = map[string]any{"wrapper_protocol": "anthropic-messages-cli-v1"}
	upstream := &anthropicHTTPUpstreamRecorder{resp: &http.Response{StatusCode: 204, Body: io.NopCloser(strings.NewReader(""))}}
	svc := &GatewayService{accountRepo: &cleanupAccountRepo{account: account}, httpUpstream: upstream, cfg: &config.Config{}}
	require.NoError(t, svc.deleteAbandonedWrapperSession(context.Background(), account.ID, "sub2api-old-generation"))
	require.Equal(t, http.MethodDelete, upstream.lastReq.Method)
	require.Equal(t, "/v1/wrapper/sessions/sub2api-old-generation", upstream.lastReq.URL.Path)
	require.Equal(t, "upstream-anthropic-key", upstream.lastReq.Header.Get("x-api-key"))
	upstream.resp = &http.Response{StatusCode: 409, Body: io.NopCloser(strings.NewReader("busy"))}
	require.ErrorIs(t, svc.deleteAbandonedWrapperSession(context.Background(), account.ID, "sub2api-old-generation"), errWrapperCleanupBusy)
	upstream.lastReq = nil
	require.Error(t, svc.deleteAbandonedWrapperSession(context.Background(), account.ID, "sub2api-../other"))
	require.Nil(t, upstream.lastReq)
	account.Extra = nil
	require.Error(t, svc.deleteAbandonedWrapperSession(context.Background(), account.ID, "sub2api-old-generation"))
	require.Nil(t, upstream.lastReq)
}

func TestWrapperManagedAbandonDeletesOldGenerationBeforeReset(t *testing.T) {
	ctx := context.Background()
	turn, err := beginWrapperManagedAutoTurn(ctx, newWrapperManagedTestStore(), WrapperManagedAutoKey(1, "cleanup"))
	require.NoError(t, err)
	defer turn.Release()
	old := wrapperManagedState{AccountID: 8, NativeSessionID: "sub2api-old-generation", Phase: "ready", PendingToolIDs: []string{"toolu_old"}}
	turn.state = old
	calls := 0
	turn.cleanupNative = func(ctx context.Context, accountID int64, id string) error {
		calls++
		require.NoError(t, ctx.Err())
		require.Equal(t, int64(8), accountID)
		require.Equal(t, old.NativeSessionID, id)
		require.Equal(t, old, turn.state)
		return errors.New("temporary cleanup failure")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	require.Error(t, turn.abandonNativeSession(cancelled, "account_changed"))
	require.Equal(t, old, turn.state)
	turn.cleanupNative = func(ctx context.Context, accountID int64, id string) error { calls++; return nil }
	require.NoError(t, turn.abandonNativeSession(ctx, "history_changed"))
	require.Equal(t, wrapperManagedState{}, turn.state)
	require.Equal(t, 2, calls)
	require.NoError(t, turn.abandonNativeSession(ctx, "empty_history"))
	require.Equal(t, 2, calls)
}
