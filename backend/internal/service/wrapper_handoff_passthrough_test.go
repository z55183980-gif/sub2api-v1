package service

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/stretchr/testify/require"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type handoffConflictUpstream struct {
	t       *testing.T
	posts   int
	corrupt bool
}

func (u *handoffConflictUpstream) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	return u.reply(req)
}
func (u *handoffConflictUpstream) DoWithTLS(req *http.Request, _ string, _ int64, _ int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return u.reply(req)
}
func (u *handoffConflictUpstream) reply(req *http.Request) (*http.Response, error) {
	u.posts++
	require.Equal(u.t, wrapperHandoffProtocol, req.Header.Get("X-Wrapper-Handoff-Protocol"))
	attempt := req.Header.Get("X-Wrapper-Handoff-Attempt")
	require.Len(u.t, attempt, 32)
	require.NotEqual(u.t, "client-forged", attempt)
	if u.corrupt {
		attempt = "wrong"
	}
	body, _ := json.Marshal(map[string]any{"type": "error", "error": map[string]any{"type": "managed_contract_conflict", "message": "tools changed", "handoff": map[string]any{"protocol": wrapperHandoffProtocol, "attempt": attempt, "session_id": req.Header.Get("X-Wrapper-Session-Id"), "receipt": req.Header.Get("X-Wrapper-Ack-Turn-Id"), "pending_tool_ids": []string{"toolu_one"}, "results_delivered": false, "session_reusable": true}}})
	return &http.Response{StatusCode: 400, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(string(body)))}, nil
}

func TestWrapperHandoffConflictPassthroughKeepsRetryableReceipt(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		ctx := context.Background()
		store := newWrapperManagedTestStore()
		turn, err := beginWrapperManagedAutoTurn(ctx, store, WrapperManagedAutoKey(42, "conflict"))
		require.NoError(t, err)
		func() {
			defer turn.Release()
			previous := wrapperManagedState{AccountID: 201, NativeSessionID: "sub2api-original", Phase: "ready", Model: "sonnet", Receipt: "previous-receipt", System: json.RawMessage(`null`), PendingToolIDs: []string{"toolu_one"}, Messages: []json.RawMessage{json.RawMessage(`{"role":"user","content":"task"}`), json.RawMessage(`{"role":"assistant","content":[{"type":"tool_use","id":"toolu_one","name":"lookup","input":{}}]}`)}}
			turn.state = previous
			c := managedTestContext()
			c.Request.Header.Set("X-Wrapper-Handoff-Attempt", "client-forged")
			body := []byte(`{"model":"sonnet","stream":true,"messages":[{"role":"user","content":"task"},{"role":"assistant","content":[{"type":"tool_use","id":"toolu_one","name":"lookup","input":{}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_one","content":"done"}]}]}`)
			parsed, err := ParseGatewayRequest(NewRequestBodyRef(body), PlatformAnthropic)
			require.NoError(t, err)
			wire, active, err := turn.Prepare(ctx, 201, body, c)
			require.NoError(t, err)
			require.True(t, active)
			parsed.AutoWrapperManagedTurn = turn
			account := newAnthropicAPIKeyAccountForTest()
			account.Extra["wrapper_protocol"] = "anthropic-messages-cli-v1"
			upstream := &handoffConflictUpstream{t: t, corrupt: corrupt}
			cfg := &config.Config{}
			svc := &GatewayService{cfg: cfg, httpUpstream: upstream, rateLimitService: NewRateLimitService(&autoManagedFailoverAccountRepo{}, nil, cfg, nil, nil)}
			_, err = svc.forwardAnthropicAPIKeyPassthroughWithInput(ctx, c, account, anthropicPassthroughForwardInput{Body: wire, Parsed: parsed, RequestModel: "sonnet", OriginalModel: "sonnet", RequestStream: true, StartTime: time.Now()})
			require.Error(t, err)
			var failover *UpstreamFailoverError
			require.False(t, errors.As(err, &failover))
			require.Equal(t, 1, upstream.posts)
			if corrupt {
				require.Equal(t, "quarantined", turn.state.Phase)
			} else {
				require.Equal(t, previous, turn.state)
			}
		}()
	}
}
