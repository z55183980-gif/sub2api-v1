package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

var wrapperExecutionID = regexp.MustCompile(`^[a-fA-F0-9-]{36}$`)

type wrapperExecutionRoute struct {
	AccountID   int64  `json:"account_id"`
	KeyID       int64  `json:"key_id"`
	ExecutionID string `json:"execution_id"`
	Scope       string `json:"scope"`
	ClientScope string `json:"client_scope"`
	Base        string `json:"base"`
	ManagedKey  string `json:"managed_key,omitempty"`
	NativeID    string `json:"native_id,omitempty"`
	PromptID    string `json:"prompt_id,omitempty"`
}

func wrapperExecutionRouteKey(keyID int64, id, scope string) string {
	return "execution-route-v1:" + strconv.FormatInt(keyID, 10) + ":" + id + ":" + HashUsageRequestPayload([]byte(scope))
}
func WrapperClientScope(header string) string {
	// Optional client scopes are opaque selectors within one API key.
	// They never grant access to another key's execution or reduce billing.
	if len(header) > 128 {
		return "invalid"
	}
	return header
}
func wrapperControlNegotiated(c *gin.Context, account *Account) bool {
	return os.Getenv("SUB2API_WRAPPER_EXECUTION_CONTROL") == "1" && account.GetExtraString("wrapper_protocol") == "anthropic-messages-cli-v1" && account.GetExtraString("wrapper_execution_control") == "v1" && strings.Contains(","+strings.ReplaceAll(c.GetHeader("X-Wrapper-Capabilities"), " ", "")+",", ",execution-control-v1,")
}

func (s *GatewayService) captureWrapperExecution(ctx context.Context, c *gin.Context, account *Account, response *http.Response) error {
	if !wrapperControlNegotiated(c, account) {
		return nil
	}
	id := response.Header.Get("X-Wrapper-Execution-Id")
	if id == "" {
		return nil
	}
	if !wrapperExecutionID.MatchString(id) {
		return errors.New("invalid Wrapper execution identity")
	}
	keyID := c.GetInt64("wrapper_execution_key_id")
	if keyID <= 0 {
		return errors.New("missing execution credential scope")
	}
	token, kind, err := s.GetAccessToken(ctx, account)
	if err != nil || kind != "apikey" {
		return errors.New("execution credentials unavailable")
	}
	r := wrapperExecutionRoute{AccountID: account.ID, KeyID: keyID, ExecutionID: id, Scope: HashUsageRequestPayload([]byte(token)), ClientScope: WrapperClientScope(c.GetHeader("X-Wrapper-Client-Scope")), Base: account.GetBaseURL()}
	r.ManagedKey = c.GetString("wrapper_managed_state_key")
	r.NativeID = c.GetHeader("X-Wrapper-Session-Id")
	r.PromptID = c.GetHeader("X-Claude-Code-Prompt-Id")
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	store := s.WrapperManagedStore()
	if store == nil {
		return errors.New("execution route store unavailable")
	}
	if err = store.SetWrapperManagedState(ctx, wrapperExecutionRouteKey(keyID, id, r.ClientScope), data, 24*time.Hour); err != nil {
		return err
	}
	for _, header := range []string{"X-Wrapper-Execution-Id", "X-Wrapper-Owner-Epoch", "X-Wrapper-Response-Step-Id"} {
		c.Header(header, response.Header.Get(header))
	}
	if strings.HasPrefix(response.Header.Get("Content-Type"), "text/event-stream") {
		c.Header("Content-Type", "text/event-stream")
		c.Writer.Flush()
	}
	return nil
}

// Authorization is against the saved key/execution binding, never a client
// supplied account ID. No model selection, generation slot, or session lock.
func (s *GatewayService) ProxyWrapperExecution(ctx context.Context, keyID int64, clientScope, id, method string, body []byte) (int, []byte, error) {
	if !wrapperExecutionID.MatchString(id) {
		return 404, nil, nil
	}
	store := s.WrapperManagedStore()
	if store == nil {
		return 503, nil, errors.New("execution store unavailable")
	}
	raw, err := store.GetWrapperManagedState(ctx, wrapperExecutionRouteKey(keyID, id, clientScope))
	if err != nil {
		return 503, nil, err
	}
	if len(raw) == 0 {
		return 404, nil, nil
	}
	var route wrapperExecutionRoute
	if json.Unmarshal(raw, &route) != nil || route.KeyID != keyID || route.ExecutionID != id || route.ClientScope != clientScope {
		return 404, nil, nil
	}
	account, err := s.accountRepo.GetByID(ctx, route.AccountID)
	if err != nil {
		return 503, nil, err
	}
	if account.GetExtraString("wrapper_protocol") != "anthropic-messages-cli-v1" || account.GetBaseURL() != route.Base {
		return 503, nil, errors.New("execution owner no longer supports Wrapper")
	}
	token, kind, err := s.GetAccessToken(ctx, account)
	if err != nil || kind != "apikey" || HashUsageRequestPayload([]byte(token)) != route.Scope {
		return 503, nil, errors.New("execution owner credentials changed")
	}
	base, err := s.validateUpstreamBaseURL(account.GetBaseURL())
	if err != nil {
		return 503, nil, err
	}
	path := "/v1/wrapper/executions/" + id
	if method == http.MethodPost {
		path += "/control"
	}
	ctx, cancel := context.WithTimeout(WithHTTPUpstreamRedirectsDisabled(ctx), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(base, "/")+path, bytes.NewReader(body))
	if err != nil {
		return 503, nil, err
	}
	setAnthropicAPIKeyAuthHeader(req.Header, account, token, account.GetBaseURL())
	account.ApplyHeaderOverrides(req.Header)
	req.Header.Set("Content-Type", "application/json")
	proxy := ""
	if account.Proxy != nil {
		proxy = account.Proxy.URL()
	}
	resp, err := s.httpUpstream.DoWithTLS(req, proxy, account.ID, account.Concurrency, s.tlsFPProfileService.ResolveTLSProfile(account))
	if err != nil {
		return 503, nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, (64<<10)+1))
	if len(data) > 64<<10 {
		return 503, nil, errors.New("execution control response too large")
	}
	if err == nil && resp.StatusCode == 200 {
		var state struct {
			State string `json:"state"`
		}
		if json.Unmarshal(data, &state) == nil && (state.State == "cancelled" || state.State == "interrupted") && route.ManagedKey != "" && route.NativeID != "" {
			// This fence survives a subsequent native generation. Otherwise a
			// late HTTP retry could rebuild the already cancelled prompt again.
			if route.PromptID != "" {
				if err = store.SetWrapperManagedState(ctx, wrapperCancelledPromptKey(route.ManagedKey, route.PromptID), []byte("1"), wrapperManagedTTL); err != nil {
					return 503, nil, err
				}
			}
			// Independent of the generation lock: the next holder consumes this
			// generation-specific fence before it can deliver any tool result.
			marker, _ := json.Marshal(map[string]string{"prompt_id": route.PromptID})
			if err = store.SetWrapperManagedState(ctx, wrapperCancelledGenerationKey(route.ManagedKey, route.NativeID), marker, wrapperManagedTTL); err != nil {
				return 503, nil, err
			}
		}
	}
	return resp.StatusCode, data, err
}

func wrapperCancelledGenerationKey(managedKey, nativeID string) string {
	return "cancelled-generation-v1:" + HashUsageRequestPayload([]byte(managedKey+":"+nativeID))
}

func wrapperCancelledPromptKey(managedKey, promptID string) string {
	return "cancelled-prompt-v1:" + HashUsageRequestPayload([]byte(managedKey+":"+promptID))
}
