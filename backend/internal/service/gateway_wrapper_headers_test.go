package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

func TestAllowedAnthropicUpstreamHeaderWrapperControls(t *testing.T) {
	wrapper := &Account{Extra: map[string]any{"wrapper_protocol": "anthropic-messages-cli-v1"}}
	ordinary := &Account{Extra: map[string]any{"wrapper_protocol": "other"}}
	for _, header := range []string{"x-wrapper-session-id", "x-wrapper-session-mode", "x-wrapper-ack-turn-id", "x-idempotency-key"} {
		if !allowedAnthropicUpstreamHeader(wrapper, header) {
			t.Fatalf("wrapper header %s should be forwarded", header)
		}
		if allowedAnthropicUpstreamHeader(ordinary, header) {
			t.Fatalf("ordinary account must not forward %s", header)
		}
	}
	if !allowedAnthropicUpstreamHeader(ordinary, "anthropic-version") {
		t.Fatal("ordinary allowed header should remain supported")
	}
}

func TestWrapperSessionHeadersForwardOnlyForWrapperAccount(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := &GatewayService{cfg: &config.Config{}}
	for _, wrapper := range []bool{false, true} {
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
		ctx.Request.Header.Set("X-Wrapper-Session-Id", "session-123")
		ctx.Request.Header.Set("X-Wrapper-Session-Mode", "managed")
		ctx.Request.Header.Set("X-Idempotency-Key", "turn-1")
		ctx.Request.Header.Set("X-Wrapper-Ack-Turn-Id", "11111111-1111-4111-8111-111111111111")
		account := newAnthropicAPIKeyAccountForTest()
		if wrapper {
			account.Extra["wrapper_protocol"] = "anthropic-messages-cli-v1"
		}
		req, _, err := svc.buildUpstreamRequestAnthropicAPIKeyPassthrough(context.Background(), ctx, account,
			[]byte(`{"model":"sonnet","max_tokens":100,"messages":[{"role":"user","content":"hello"}]}`), "upstream-key")
		if err != nil {
			t.Fatal(err)
		}
		for _, header := range []string{"X-Wrapper-Session-Id", "X-Wrapper-Session-Mode", "X-Wrapper-Ack-Turn-Id", "X-Idempotency-Key"} {
			if got := req.Header.Get(header); wrapper && got == "" {
				t.Fatalf("wrapper account did not forward %s", header)
			} else if !wrapper && got != "" {
				t.Fatalf("ordinary account forwarded %s", header)
			}
		}
	}
}

func TestManagedWrapperSessionHashIgnoresIncrementalMessageBody(t *testing.T) {
	svc := &GatewayService{}
	context := &SessionContext{APIKeyID: 42, WrapperSessionID: "conversation-1"}
	first := &ParsedRequest{Body: NewRequestBodyRef([]byte(`{"messages":[{"role":"user","content":"first"}]}`)), SessionContext: context}
	second := &ParsedRequest{Body: NewRequestBodyRef([]byte(`{"messages":[{"role":"user","content":"second"}]}`)), SessionContext: context}
	if svc.GenerateSessionHash(first) != svc.GenerateSessionHash(second) {
		t.Fatal("managed wrapper turns must keep one sticky account")
	}
	other := &ParsedRequest{Body: first.Body, SessionContext: &SessionContext{APIKeyID: 43, WrapperSessionID: "conversation-1"}}
	if svc.GenerateSessionHash(first) == svc.GenerateSessionHash(other) {
		t.Fatal("different API keys must not share wrapper session routing")
	}
	if SanitizeWrapperSessionID(" conversation-1 ") != "" || SanitizeWrapperSessionID("conversation-1") != "conversation-1" {
		t.Fatal("wrapper session ID validation must match the Wrapper")
	}
}

func TestWrapperTerminalReceiptSurvivesPassthroughStreaming(t *testing.T) {
	gin.SetMode(gin.TestMode)
	svc := newStreamingResponseTestGatewayService()
	recorder := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(recorder)
	ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	const receipt = "11111111-1111-4111-8111-111111111111"
	response := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
		Body: io.NopCloser(strings.NewReader("event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":2}}\n\n" +
			"event: message_delta\ndata: {\"type\":\"message_delta\",\"usage\":{\"output_tokens\":3}}\n\n" +
			"event: message_stop\ndata: {\"type\":\"message_stop\",\"wrapper_turn_id\":\"" + receipt + "\"}\n\n")),
	}
	_, err := svc.handleStreamingResponseAnthropicAPIKeyPassthrough(context.Background(), response, ctx,
		&Account{ID: 1}, time.Now(), "sonnet")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(recorder.Body.String(), `"wrapper_turn_id":"`+receipt+`"`) {
		t.Fatal("Wrapper terminal receipt did not reach the client")
	}
}

// SUB2API_WRAPPER_PROBE_URL points only at a separately launched local
// Wrapper instance. This exercises the actual passthrough request builder and
// streaming pump without starting the production gateway service.
func TestIsolatedWrapperCLIThroughSub2APIPassthrough(t *testing.T) {
	baseURL := os.Getenv("SUB2API_WRAPPER_PROBE_URL")
	if baseURL == "" {
		t.Skip("isolated Wrapper probe URL not set")
	}
	gin.SetMode(gin.TestMode)
	cfg := &config.Config{}
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	svc := &GatewayService{cfg: cfg, httpUpstream: &wrapperProbeHTTPUpstream{}, rateLimitService: &RateLimitService{}}
	account := newAnthropicAPIKeyAccountForTest()
	account.Credentials["base_url"] = baseURL
	account.Extra["wrapper_protocol"] = "anthropic-messages-cli-v1"
	const sessionID = "sub2api-wrapper-probe"
	var receipt string
	var toolID string
	for round := 0; round < 2; round++ {
		body := `{"model":"sonnet","max_tokens":100,"stream":true,"messages":[{"role":"user","content":"Call lookup exactly once with key alpha; then answer briefly."}],"tools":[{"name":"lookup","description":"Look up a key","input_schema":{"type":"object","properties":{"key":{"type":"string"}},"required":["key"]}}]}`
		if round == 1 {
			body = `{"model":"sonnet","max_tokens":100,"stream":true,"messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"` + toolID + `","content":"GATEWAY_REAL_ALPHA_42"}]}]}`
		}
		recorder := httptest.NewRecorder()
		ctx, _ := gin.CreateTestContext(recorder)
		ctx.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(body))
		ctx.Request.Header.Set("X-Wrapper-Session-Mode", "managed")
		ctx.Request.Header.Set("X-Wrapper-Session-Id", sessionID)
		if round == 1 {
			ctx.Request.Header.Set("X-Wrapper-Ack-Turn-Id", receipt)
		}
		parsed, err := ParseGatewayRequest(NewRequestBodyRef([]byte(body)), PlatformAnthropic)
		if err != nil {
			t.Fatal(err)
		}
		result, err := svc.Forward(context.Background(), ctx, account, parsed)
		if err != nil {
			t.Fatal(err)
		}
		if result == nil || !result.Stream {
			t.Fatal("streaming passthrough did not complete")
		}
		output := recorder.Body.String()
		if round == 0 {
			for _, event := range strings.Split(output, "\n\n") {
				data := extractSSEDataByType(event, "content_block_start")
				if gjson.Get(data, "content_block.type").String() == "tool_use" {
					toolID = gjson.Get(data, "content_block.id").String()
					break
				}
			}
			if toolID == "" || !strings.Contains(output, `"stop_reason":"tool_use"`) {
				t.Fatalf("missing tool call: %s", output)
			}
		} else {
			var answer strings.Builder
			for _, event := range strings.Split(output, "\n\n") {
				data := strings.TrimSpace(extractSSEDataByType(event, "content_block_delta"))
				if data != "" {
					answer.WriteString(gjson.Get(data, "delta.text").String())
				}
			}
			if !strings.Contains(answer.String(), "GATEWAY_REAL_ALPHA_42") {
				t.Fatalf("real result missing from assistant answer: %s", answer.String())
			}
		}
		receipt = gjson.Get(extractSSEDataByType(output, "message_stop"), "wrapper_turn_id").String()
		if receipt == "" {
			t.Fatal("terminal receipt was not delivered through gateway stream")
		}
		t.Logf("round=%d request_id=%s receipt=true tool_id=%s usage_input=%d usage_output=%d", round+1,
			result.RequestID, toolID, result.Usage.InputTokens, result.Usage.OutputTokens)
	}
}

type wrapperProbeHTTPUpstream struct{}

func (*wrapperProbeHTTPUpstream) Do(req *http.Request, _ string, _ int64, _ int) (*http.Response, error) {
	return http.DefaultClient.Do(req)
}
func (u *wrapperProbeHTTPUpstream) DoWithTLS(req *http.Request, proxyURL string, accountID int64, concurrency int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	return u.Do(req, proxyURL, accountID, concurrency)
}

func extractSSEDataByType(stream, eventType string) string {
	for _, event := range strings.Split(stream, "\n\n") {
		for _, line := range strings.Split(event, "\n") {
			if strings.HasPrefix(line, "data: ") && gjson.Get(strings.TrimPrefix(line, "data: "), "type").String() == eventType {
				return strings.TrimPrefix(line, "data: ")
			}
		}
	}
	return ""
}
