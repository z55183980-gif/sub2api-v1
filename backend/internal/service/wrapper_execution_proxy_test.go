package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

type executionRouteStore struct {
	GatewayCache
	*wrapperManagedTestStore
}

type failingExecutionRouteStore struct {
	*executionRouteStore
	failPrefix string
}

func (s *failingExecutionRouteStore) SetWrapperManagedState(ctx context.Context, key string, value []byte, ttl time.Duration) error {
	if strings.HasPrefix(key, s.failPrefix) {
		return errors.New("synthetic fence store failure")
	}
	return s.executionRouteStore.SetWrapperManagedState(ctx, key, value, ttl)
}

func TestWrapperControlTerminalFenceIsDurableBeforeConfirmation(t *testing.T) {
	for _, state := range []string{"cancelled", "interrupted", "interrupting", "unknown", "completed"} {
		t.Run(state, func(t *testing.T) {
			ctx := context.Background()
			store := &executionRouteStore{wrapperManagedTestStore: newWrapperManagedTestStore()}
			account := newAnthropicAPIKeyAccountForTest()
			account.Extra["wrapper_protocol"] = "anthropic-messages-cli-v1"
			id := "11111111-1111-4111-8111-111111111111"
			route := wrapperExecutionRoute{AccountID: account.ID, KeyID: 42, ExecutionID: id, Scope: HashUsageRequestPayload([]byte("upstream-anthropic-key")), Base: account.GetBaseURL(), ManagedKey: "managed-key", NativeID: "old-native", PromptID: "old-prompt"}
			raw, err := json.Marshal(route)
			require.NoError(t, err)
			require.NoError(t, store.SetWrapperManagedState(ctx, wrapperExecutionRouteKey(42, id, ""), raw, time.Hour))
			upstream := &anthropicHTTPUpstreamRecorder{}
			svc := &GatewayService{cfg: &config.Config{}, cache: store, accountRepo: &lifecycleLiveAccountRepo{account: account}, httpUpstream: upstream}
			for _, method := range []string{http.MethodPost, http.MethodGet} {
				upstream.resp = &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"state":"` + state + `"}`))}
				status, _, err := svc.ProxyWrapperExecution(ctx, 42, "", id, method, nil)
				require.NoError(t, err)
				require.Equal(t, 200, status)
				generation, _ := store.GetWrapperManagedState(ctx, wrapperCancelledGenerationKey(route.ManagedKey, route.NativeID))
				prompt, _ := store.GetWrapperManagedState(ctx, wrapperCancelledPromptKey(route.ManagedKey, route.PromptID))
				want := state == "cancelled" || state == "interrupted"
				require.Equal(t, want, len(generation) > 0)
				require.Equal(t, want, len(prompt) > 0)
			}
			if state == "cancelled" {
				for _, prefix := range []string{"cancelled-prompt-v1:", "cancelled-generation-v1:"} {
					svc.cache = &failingExecutionRouteStore{executionRouteStore: store, failPrefix: prefix}
					upstream.resp = &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"state":"cancelled"}`))}
					status, out, err := svc.ProxyWrapperExecution(ctx, 42, "", id, http.MethodGet, nil)
					require.Error(t, err)
					require.Equal(t, 503, status)
					require.Nil(t, out, "must not acknowledge termination until retry fencing is saved")
				}
			}
		})
	}
}

func TestCancelledPromptStaysFencedAfterNativeGenerationChanges(t *testing.T) {
	ctx := context.Background()
	store := newWrapperManagedTestStore()
	key := WrapperManagedAutoKey(42, "late-retry")
	turn, err := beginWrapperManagedAutoTurn(ctx, store, key)
	require.NoError(t, err)
	defer turn.Release()
	body := []byte(`{"model":"sonnet","stream":true,"messages":[{"role":"user","content":"new request"}]}`)
	c := managedTestContext()
	c.Request.Header.Set("X-Claude-Code-Prompt-Id", "new-prompt")
	_, active, err := turn.Prepare(ctx, 8, body, c)
	require.NoError(t, err)
	require.True(t, active)
	newNativeID := turn.state.NativeSessionID
	require.NoError(t, store.SetWrapperManagedState(ctx, wrapperCancelledGenerationKey(key, "old-native"), []byte(`{"prompt_id":"old-prompt"}`), time.Hour))
	require.NoError(t, store.SetWrapperManagedState(ctx, wrapperCancelledPromptKey(key, "old-prompt"), []byte("1"), time.Hour))
	c.Request.Header.Set("X-Claude-Code-Prompt-Id", "old-prompt")
	_, _, err = turn.Prepare(ctx, 8, body, c)
	var payloadError *WrapperManagedPayloadError
	require.ErrorAs(t, err, &payloadError)
	require.Equal(t, 409, payloadError.Status)
	require.Equal(t, newNativeID, turn.state.NativeSessionID)
	require.Equal(t, "pending", turn.state.Phase)
}

func TestCancelledGenerationDoesNotDeliverLateToolsAndAllowsNewPrompt(t *testing.T) {
	for _, fresh := range []bool{false, true} {
		t.Run(map[bool]string{false: "late_result", true: "new_prompt"}[fresh], func(t *testing.T) {
			ctx := context.Background()
			store := newWrapperManagedTestStore()
			key := WrapperManagedAutoKey(42, "cancelled-tools")
			turn, err := beginWrapperManagedAutoTurn(ctx, store, key)
			require.NoError(t, err)
			defer turn.Release()
			turn.state = wrapperManagedState{AccountID: 8, NativeSessionID: "sub2api-old", Phase: "ready", PendingToolIDs: []string{"tool-1"}}
			require.NoError(t, store.SetWrapperManagedState(ctx, wrapperCancelledGenerationKey(key, "sub2api-old"), []byte(`{"prompt_id":"old-prompt"}`), time.Hour))
			cleaned := false
			turn.cleanupNative = func(context.Context, int64, string) error { cleaned = true; return nil }
			turn.PreserveBranch()
			c := managedTestContext()
			c.Request.Header.Set("X-Claude-Code-Request-Class", "main")
			c.Request.Header.Set("X-Claude-Code-Prompt-Id", "old-prompt")
			if fresh {
				c.Request.Header.Set("X-Claude-Code-Prompt-Id", "new-prompt")
			}
			body := []byte(`{"model":"sonnet","stream":true,"messages":[{"role":"user","content":"test"},{"role":"assistant","content":[{"type":"tool_use","id":"tool-1","name":"Read","input":{}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool-1","is_error":true,"content":"interrupted"},{"type":"text","text":"continue"}]}]}`)
			wire, active, err := turn.Prepare(ctx, 8, body, c)
			if !fresh {
				require.Error(t, err)
				require.False(t, cleaned)
				require.Equal(t, "sub2api-old", turn.state.NativeSessionID)
				return
			}
			require.NoError(t, err)
			require.True(t, active)
			require.True(t, cleaned)
			require.NotEqual(t, "sub2api-old", turn.state.NativeSessionID)
			require.NotEmpty(t, wire)
			require.Empty(t, c.GetHeader("X-Wrapper-Ack-Turn-Id"))
		})
	}
}

func TestWrapperExecutionRoutingRejectsForeignKeysBeforeAccountLookup(t *testing.T) {
	store := &executionRouteStore{wrapperManagedTestStore: newWrapperManagedTestStore()}
	svc := &GatewayService{cache: store}
	id := "11111111-1111-4111-8111-111111111111"
	route := wrapperExecutionRoute{AccountID: 9, KeyID: 42, ExecutionID: id, ClientScope: "token-a"}
	raw, _ := json.Marshal(route)
	store.states[wrapperExecutionRouteKey(42, id, "token-a")] = raw
	for _, scope := range []struct {
		key   int64
		scope string
	}{{43, "token-a"}, {42, "token-b"}, {42, ""}} {
		status, _, err := svc.ProxyWrapperExecution(context.Background(), scope.key, scope.scope, id, "POST", nil)
		require.NoError(t, err)
		require.Equal(t, 404, status)
	}
}
func TestWrapperExecutionCapabilitiesRequireAccountAndClientNegotiation(t *testing.T) {
	t.Setenv("SUB2API_WRAPPER_EXECUTION_CONTROL", "1")
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
	c.Request.Header.Set("X-Wrapper-Capabilities", "execution-control-v1")
	account := newAnthropicAPIKeyAccountForTest()
	account.Extra["wrapper_protocol"] = "anthropic-messages-cli-v1"
	svc := &GatewayService{cfg: &config.Config{}}
	for _, advertised := range []bool{false, true} {
		if advertised {
			account.Extra["wrapper_execution_control"] = "v1"
		}
		req, _, err := svc.buildUpstreamRequestAnthropicAPIKeyPassthrough(context.Background(), c, account, []byte(`{"model":"sonnet","stream":true,"messages":[{"role":"user","content":"hi"}]}`), "test-key")
		require.NoError(t, err)
		require.Equal(t, advertised, req.Header.Get("X-Wrapper-Capabilities") == "execution-control-v1")
	}
}
