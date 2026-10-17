package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/Wei-Shaw/sub2api/internal/pkg/tlsfingerprint"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Fault injection is confined to isolated wrappers and an in-memory repository.
// The first failure is synthetic; the replacement and tool continuation use a real CLI.
type bridgeLiveHTTP struct {
	inject bool
	count  int
}

func (u *bridgeLiveHTTP) DoWithTLS(req *http.Request, _ string, accountID int64, _ int, _ *tlsfingerprint.Profile) (*http.Response, error) {
	port := 24732
	if accountID == 2 {
		port = 24732
	}
	req.URL.Host = fmt.Sprintf("127.0.0.1:%d", port)
	req.URL.Scheme = "http"
	req.Host = req.URL.Host
	if u.inject {
		u.inject = false
		u.count++
		return &http.Response{StatusCode: 200, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(quotaSSE(quotaStart, `{"type":"error","error":{"type":"client_tool_bridge_violation","retry_safe":true}}`)))}, nil
	}
	return http.DefaultClient.Do(req)
}

func (u *bridgeLiveHTTP) Do(req *http.Request, proxy string, accountID int64, concurrency int) (*http.Response, error) {
	return u.DoWithTLS(req, proxy, accountID, concurrency, nil)
}

func TestWrapperToolBridgeLive(t *testing.T) {
	if os.Getenv("WRAPPER_BRIDGE_LIVE") != "1" {
		t.Skip("isolated real CLI integration disabled")
	}
	dir := os.Getenv("WRAPPER_BRIDGE_EVIDENCE")
	if dir == "" {
		t.Fatal("evidence path required")
	}
	os.MkdirAll(dir, 0700)
	gin.SetMode(gin.TestMode)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()
	cfg := &config.Config{}
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	transport := &bridgeLiveHTTP{inject: true}
	repo := &wrapperQuotaRepo{}
	svc := &GatewayService{cfg: cfg, httpUpstream: transport, rateLimitService: &RateLimitService{accountRepo: repo}}
	store := newWrapperManagedTestStore()
	key := "quota-live-" + fmt.Sprint(time.Now().UnixNano())
	native := map[string]int64{}
	defer func() {
		for sid, id := range native {
			port := 24732
			if id == 2 {
				port = 24732
			}
			req, _ := http.NewRequest("DELETE", fmt.Sprintf("http://127.0.0.1:%d/v1/wrapper/sessions/%s", port, sid), nil)
			res, err := http.DefaultClient.Do(req)
			if err == nil {
				res.Body.Close()
			}
		}
	}()
	body := []byte(`{"model":"claude-opus-5-5","max_tokens":2048,"stream":true,"system":"You are running an isolated continuity test. Follow the user's exact steps. Tools execute only on the client.","messages":[{"role":"user","content":"Write exactly: QUOTA_PREFIX The client owns all tool operations. Then call Read with path probe.txt exactly once. After receiving its result, reply QUOTA_RECOVERY_OK and the marker from the result. Do not perform any other tool operations."}],"tools":[{"name":"Read","description":"Read a synthetic client file","input_schema":{"type":"object","properties":{"path":{"type":"string"}},"required":["path"]}}]}`)
	original := append([]byte(nil), body...)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("POST", "/v1/messages", nil).WithContext(ctx)
	EnableWrapperStreamRecovery(c, body)
	defer FinishWrapperStreamRecovery(c)
	var retained *WrapperManagedAutoTurn
	attempt := func(id int64, body []byte, c *gin.Context) (*WrapperManagedAutoTurn, error) {
		turn := retained
		var err error
		if turn == nil {
			turn, err = beginWrapperManagedAutoTurn(ctx, store, key)
		}
		if err != nil {
			t.Fatal(err)
		}
		c.Request.Header.Del("X-Wrapper-Ack-Turn-Id")
		wire, active, err := turn.Prepare(ctx, id, body, c)
		if err != nil || !active {
			t.Fatalf("prepare active=%v err=%v reason=%s", active, err, turn.skipReason)
		}
		native[turn.state.NativeSessionID] = id
		a := newAnthropicAPIKeyAccountForTest()
		a.ID = id
		a.Credentials["base_url"] = "http://127.0.0.1:24732"
		a.Extra["wrapper_protocol"] = "anthropic-messages-cli-v1"
		parsed, err := ParseGatewayRequest(NewRequestBodyRef(wire), PlatformAnthropic)
		if err != nil {
			t.Fatal(err)
		}
		parsed.AutoWrapperManagedTurn = turn
		_, err = svc.Forward(ctx, c, a, parsed)
		return turn, err
	}
	first, err := attempt(1, body, c)
	var quotaErr *WrapperToolBridgeError
	if !errors.As(err, &quotaErr) {
		first.Release()
		t.Fatalf("injection did not reach recovery: %v", err)
	}
	first.Quarantine()
	retained = first
	first.ResetBridgeAttempt()
	body, err = AdvanceWrapperToolBridgeRecovery(c, quotaErr)
	if err != nil {
		t.Fatal(err)
	}
	second, err := attempt(1, body, c)
	if err != nil {
		second.Release()
		t.Fatal(err)
	}
	second.Release()
	retained = nil
	os.WriteFile(filepath.Join(dir, "stitched.sse"), recorder.Body.Bytes(), 0600)
	r := wrapperRecovery(c)
	var tools []json.RawMessage
	for _, b := range r.blocks {
		if gjson.GetBytes(b, "type").String() == "tool_use" {
			tools = append(tools, b)
		}
	}
	if len(tools) != 1 {
		t.Fatalf("expected exactly one delivered tool: %d", len(tools))
	}
	if strings.Count(recorder.Body.String(), "event: message_start") != 1 || strings.Count(recorder.Body.String(), "event: message_stop") != 1 {
		t.Fatal("invalid stream lifecycle")
	}
	var messages []json.RawMessage
	json.Unmarshal([]byte(gjson.GetBytes(original, "messages").Raw), &messages)
	assistant, _ := json.Marshal(map[string]any{"role": "assistant", "content": r.blocks})
	result, _ := json.Marshal(map[string]any{"role": "user", "content": []any{map[string]any{"type": "tool_result", "tool_use_id": gjson.GetBytes(tools[0], "id").String(), "content": "SYNTHETIC_TOOL_MARKER"}}})
	messages = append(messages, assistant, result)
	next, _ := sjson.SetBytes(original, "messages", messages)
	recorder2 := httptest.NewRecorder()
	c2, _ := gin.CreateTestContext(recorder2)
	c2.Request = httptest.NewRequest("POST", "/v1/messages", nil).WithContext(ctx)
	EnableWrapperStreamRecovery(c2, next)
	defer FinishWrapperStreamRecovery(c2)
	third, err := attempt(1, next, c2)
	if err != nil {
		third.Release()
		t.Fatal(err)
	}
	if !third.wasReady {
		t.Fatal("tool result rebuilt instead of resuming receipt")
	}
	third.Release()
	os.WriteFile(filepath.Join(dir, "tool-result.sse"), recorder2.Body.Bytes(), 0600)
	if !strings.Contains(wrapperRecovery(c2).text, "SYNTHETIC_TOOL_MARKER") || strings.Contains(recorder2.Body.String(), `"type":"tool_use"`) {
		t.Fatal("tool result continuation failed or repeated tool")
	}
	evidence, _ := json.MarshalIndent(map[string]any{"injected": transport.count, "cooldownWrites": repo.writes, "deliveredTools": len(tools), "nativeSessions": len(native), "resumedReceipt": third.wasReady, "bridgeRetries": r.bridgeRetries, "firstVisibleText": r.text}, "", "  ")
	os.WriteFile(filepath.Join(dir, "verification.json"), evidence, 0600)
	t.Log(string(evidence))
}
