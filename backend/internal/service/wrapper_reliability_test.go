package service

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestWrapperStatelessAffinityMovingCacheMarkersKeepsAccount(t *testing.T) {
	svc := &GatewayService{}
	first := []byte(`{"model":"sonnet","system":[{"type":"text","text":"x-anthropic-billing-header: cch=first;"},{"type":"text","text":"Fixed policy","cache_control":{"type":"ephemeral"}}],"messages":[{"role":"user","content":[{"type":"text","text":"task","cache_control":{"type":"ephemeral"}}]}]}`)
	initial := svc.ResolveWrapperStatelessAffinity(42, nil, first)
	svc.RememberWrapperStatelessAffinity(initial, 8)
	withoutHints := []byte(`{"model":"sonnet","system":[{"type":"text","text":"Fixed policy"}],"messages":[{"role":"user","content":[{"type":"text","text":"task"}]}]}`)
	require.Equal(t, initial.Key, svc.ResolveWrapperStatelessAffinity(42, nil, withoutHints).Key)
	grown := []byte(`{"model":"sonnet","system":[{"type":"text","text":"x-anthropic-billing-header: cch=second;"},{"type":"text","text":"Fixed policy"}],"messages":[{"role":"user","content":[{"type":"text","text":"task"}]},{"role":"assistant","content":"done"},{"role":"user","content":[{"type":"text","text":"next","cache_control":{"type":"ephemeral","ttl":"1h"}}]}]}`)
	next := svc.ResolveWrapperStatelessAffinity(42, nil, grown)
	require.EqualValues(t, 8, next.AccountID)
	require.Equal(t, 1, next.MatchedMessages)
	changed := []byte(strings.Replace(string(grown), `"text":"task"`, `"text":"other task"`, 1))
	require.Zero(t, svc.ResolveWrapperStatelessAffinity(42, nil, changed).AccountID)
	changed = []byte(strings.Replace(string(grown), "Fixed policy", "Changed policy", 1))
	require.Zero(t, svc.ResolveWrapperStatelessAffinity(42, nil, changed).AccountID)
}

func TestWrapperStatelessCacheHintsPreserveToolInputsAndResultText(t *testing.T) {
	svc := &GatewayService{}
	withHints := []byte(`{"model":"m","messages":[{"role":"assistant","content":[{"type":"tool_use","id":"tool","name":"lookup","input":{"id":9007199254740993,"cache_control":{"value":1}},"cache_control":{"type":"ephemeral"}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool","content":[{"type":"text","text":"cache_control: actual result","cache_control":{"type":"ephemeral"}}],"cache_control":{"type":"ephemeral"}}]}]}`)
	withoutHints := []byte(`{"model":"m","messages":[{"role":"assistant","content":[{"type":"tool_use","id":"tool","name":"lookup","input":{"id":9007199254740993,"cache_control":{"value":1}}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool","content":[{"type":"text","text":"cache_control: actual result"}]}]}]}`)
	initial := svc.ResolveWrapperStatelessAffinity(42, nil, withHints)
	svc.RememberWrapperStatelessAffinity(initial, 8)
	require.Equal(t, initial.Key, svc.ResolveWrapperStatelessAffinity(42, nil, withoutHints).Key)
	for _, change := range [][2]string{{"9007199254740993", "9007199254740992"}, {`"value":1`, `"value":2`}, {"actual result", "different result"}, {`"tool_use_id":"tool"`, `"tool_use_id":"another"`}} {
		changed := []byte(strings.Replace(string(withHints), change[0], change[1], 1))
		require.Zero(t, svc.ResolveWrapperStatelessAffinity(42, nil, changed).AccountID)
	}
}

func TestWrapperManagedBillingTelemetryRetainsNativeGeneration(t *testing.T) {
	ctx := context.Background()
	turn, err := beginWrapperManagedAutoTurn(ctx, newWrapperManagedTestStore(), WrapperManagedAutoKey(42, "billing"))
	require.NoError(t, err)
	defer turn.Release()
	// A legacy snapshot still contains the previous request's telemetry.
	turn.state = wrapperManagedState{AccountID: 201, NativeSessionID: "sub2api-original", Phase: "ready", Model: "sonnet", Receipt: "previous-receipt",
		System:   json.RawMessage(`[{"type":"text","text":"x-anthropic-billing-header: cch=first;"},{"type":"text","text":"Fixed policy"}]`),
		Messages: []json.RawMessage{json.RawMessage(`{"role":"user","content":"task"}`), json.RawMessage(`{"role":"assistant","content":"done"}`)},
	}
	deletes := 0
	turn.cleanupNative = func(context.Context, int64, string) error { deletes++; return nil }
	body := []byte(`{"model":"sonnet","system":[{"type":"text","text":"x-anthropic-billing-header: cch=second;"},{"type":"text","text":"Fixed policy"}],"messages":[{"role":"user","content":"task"},{"role":"assistant","content":"done"},{"role":"user","content":"next"}]}`)
	c := managedTestContext()
	wire, active, err := turn.Prepare(ctx, 201, body, c)
	require.NoError(t, err)
	require.True(t, active)
	require.Zero(t, deletes)
	require.Equal(t, "sub2api-original", turn.state.NativeSessionID)
	require.Equal(t, "previous-receipt", c.GetHeader("X-Wrapper-Ack-Turn-Id"))
	require.EqualValues(t, 1, gjson.GetBytes(wire, "messages.#").Int())
	require.NotContains(t, string(wire), "x-anthropic-billing-header")
	require.NoError(t, turn.RestoreUndispatched(ctx))
	changed := []byte(strings.Replace(string(body), "Fixed policy", "Changed policy", 1))
	_, active, err = turn.Prepare(ctx, 201, changed, managedTestContext())
	require.NoError(t, err)
	require.True(t, active)
	require.Equal(t, 1, deletes)
	require.NotEqual(t, "sub2api-original", turn.state.NativeSessionID)
}

func TestWrapperSystemTelemetryKeepsQuotedPolicyAndOrder(t *testing.T) {
	for _, pair := range [][2]string{
		{`[{"type":"text","text":"x-anthropic-billing-header: example\nPolicy"}]`, `[{"type":"text","text":"x-anthropic-billing-header: example\nChanged"}]`},
		{`"x-anthropic-billing-header: string policy"`, `"x-anthropic-billing-header: changed policy"`},
		{`[{"type":"text","text":"one"},{"type":"text","text":"two"}]`, `[{"type":"text","text":"two"},{"type":"text","text":"one"}]`},
	} {
		require.False(t, equalManagedSystem(json.RawMessage(pair[0]), json.RawMessage(pair[1])))
	}
}

func TestWrapperManagedBranchRetryIgnoresBillingFingerprint(t *testing.T) {
	ctx := context.Background()
	store := newWrapperManagedTestStore()
	body := []byte(`{"model":"sonnet","system":[{"type":"text","text":"x-anthropic-billing-header: cch=first;"},{"type":"text","text":"Fixed policy"}],"messages":[{"role":"user","content":"task"}]}`)
	id, source, err := resolveWrapperManagedBranch(ctx, store, 42, "parent", body, "", branchUA)
	require.NoError(t, err)
	require.Equal(t, "new_history", source)
	turn, err := beginWrapperManagedAutoTurn(ctx, store, WrapperManagedAutoKey(42, id))
	require.NoError(t, err)
	defer turn.Release()
	_, active, err := turn.Prepare(ctx, 201, body, managedTestContext())
	require.NoError(t, err)
	require.True(t, active)
	retry := []byte(strings.Replace(string(body), "cch=first", "cch=second", 1))
	nextID, source, err := resolveWrapperManagedBranch(ctx, store, 42, "parent", retry, "", branchUA)
	require.NoError(t, err)
	require.Equal(t, "request_retry", source)
	require.Equal(t, id, nextID)
}

type admissionRaceUpstream struct {
	anthropicHTTPUpstreamRecorder
	status        int
	body          string
	probes, posts int
}

func (u *admissionRaceUpstream) DoWithTLS(req *http.Request, _ string, _ int64, _ int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	if req.Method == http.MethodGet && req.URL.Path == "/health" {
		u.probes++
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(`{"availableProcesses":1}`))}, nil
	}
	u.posts++
	return &http.Response{StatusCode: u.status, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(u.body))}, nil
}

func TestWrapperAdmissionRaceRestoresOnlyCertifiedUndispatchedTurns(t *testing.T) {
	for _, tc := range []struct {
		name, body           string
		status               int
		registered, restored bool
	}{
		{"admission lost after health", `{"type":"error","error":{"type":"wrapper_admission_rejected","request_dispatched":false,"message":"capacity"}}`, 503, true, true},
		{"ordinary 503", `{"type":"error","error":{"type":"service_unavailable","message":"capacity"}}`, 503, true, false},
		{"missing flag", `{"type":"error","error":{"type":"wrapper_admission_rejected"}}`, 503, true, false},
		{"already dispatched", `{"type":"error","error":{"type":"wrapper_admission_rejected","request_dispatched":true}}`, 503, true, false},
		{"wrong status", `{"type":"error","error":{"type":"wrapper_admission_rejected","request_dispatched":false}}`, 502, true, false},
		{"not a registered wrapper", `{"type":"error","error":{"type":"wrapper_admission_rejected","request_dispatched":false}}`, 503, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			store := newWrapperManagedTestStore()
			key := WrapperManagedAutoKey(42, tc.name)
			turn, err := beginWrapperManagedAutoTurn(ctx, store, key)
			require.NoError(t, err)
			defer turn.Release()
			previous := wrapperManagedState{AccountID: 201, NativeSessionID: "sub2api-original", Phase: "ready", Model: "sonnet", Receipt: "previous-receipt",
				System:   json.RawMessage(`null`),
				Messages: []json.RawMessage{json.RawMessage(`{"role":"user","content":"task"}`), json.RawMessage(`{"role":"assistant","content":"done"}`)},
			}
			turn.state = previous
			c := managedTestContext()
			body := []byte(`{"model":"sonnet","stream":true,"messages":[{"role":"user","content":"task"},{"role":"assistant","content":"done"},{"role":"user","content":"next"}]}`)
			parsed, err := ParseGatewayRequest(NewRequestBodyRef(body), PlatformAnthropic)
			require.NoError(t, err)
			wire, active, err := turn.Prepare(ctx, 201, body, c)
			require.NoError(t, err)
			require.True(t, active)
			parsed.AutoWrapperManagedTurn = turn
			account := newAnthropicAPIKeyAccountForTest()
			if tc.registered {
				account.Extra["wrapper_protocol"] = "anthropic-messages-cli-v1"
			}
			upstream := &admissionRaceUpstream{status: tc.status, body: tc.body}
			cfg := &config.Config{}
			svc := &GatewayService{cfg: cfg, httpUpstream: upstream, rateLimitService: NewRateLimitService(&autoManagedFailoverAccountRepo{}, nil, cfg, nil, nil)}
			result, err := svc.forwardAnthropicAPIKeyPassthroughWithInput(ctx, c, account, anthropicPassthroughForwardInput{
				Body: wire, Parsed: parsed, RequestModel: "sonnet", OriginalModel: "sonnet", RequestStream: true, StartTime: time.Now(),
			})
			require.Nil(t, result)
			var failover *UpstreamFailoverError
			require.ErrorAs(t, err, &failover)
			require.Equal(t, tc.restored, failover.WrapperNotDispatched)
			require.Equal(t, 1, upstream.posts)
			if tc.registered {
				require.Equal(t, 1, upstream.probes)
			}
			require.False(t, c.Writer.Written())
			if !tc.restored {
				require.Equal(t, "quarantined", turn.state.Phase)
				return
			}
			require.Equal(t, previous, turn.state)
			saved, err := store.GetWrapperManagedState(ctx, key)
			require.NoError(t, err)
			require.Equal(t, "ready", gjson.GetBytes(saved, "phase").String())
			// Retrying on the same node reuses the receipt and sends only new input.
			c = managedTestContext()
			wire, active, err = turn.Prepare(ctx, 201, body, c)
			require.NoError(t, err)
			require.True(t, active)
			require.Equal(t, previous.NativeSessionID, turn.state.NativeSessionID)
			require.Equal(t, previous.Receipt, c.GetHeader("X-Wrapper-Ack-Turn-Id"))
			require.EqualValues(t, 1, gjson.GetBytes(wire, "messages.#").Int())
		})
	}
}
