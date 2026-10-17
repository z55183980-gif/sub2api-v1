package service

import (
	"context"
	"errors"
	"github.com/gin-gonic/gin"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestWrapperToolBridgeRecoveryBoundaries(t *testing.T) {
	for _, test := range []struct {
		name    string
		content []string
		safe    bool
		body    string
	}{
		{"no content", nil, true, ""},
		{"text published", quotaText(0, "already visible"), false, ""},
		{"thinking published", []string{`{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}}`, `{"type":"content_block_stop","index":0}`}, false, ""},
		{"tool result continuation", nil, false, `{"messages":[{"role":"assistant","content":[{"type":"tool_use","id":"toolu_old","name":"Read","input":{}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_old","content":"done"}]}]}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
			body := test.body
			if body == "" {
				body = `{"messages":[{"role":"user","content":"hello"}]}`
			}
			EnableWrapperStreamRecovery(c, []byte(body))
			defer FinishWrapperStreamRecovery(c)
			events := append([]string{quotaStart}, test.content...)
			events = append(events, `{"type":"error","error":{"type":"client_tool_bridge_violation","retry_safe":true}}`)
			svc := &GatewayService{}
			_, err := svc.streamWrapperWithRecovery(context.Background(), &http.Response{Header: http.Header{}, Body: io.NopCloser(strings.NewReader(quotaSSE(events...)))}, c, newAnthropicAPIKeyAccountForTest(), time.Now(), nil, wrapperRecovery(c))
			var bridge *WrapperToolBridgeError
			if !errors.As(err, &bridge) {
				t.Fatalf("wrong error: %v", err)
			}
			next, err := AdvanceWrapperToolBridgeRecovery(c, bridge)
			if (err == nil) != test.safe {
				t.Fatalf("safe=%v err=%v", test.safe, err)
			}
			if test.safe {
				if string(next) != body {
					t.Fatal("history changed")
				}
				if _, err = AdvanceWrapperToolBridgeRecovery(c, bridge); err == nil {
					t.Fatal("repeated recovery")
				}
			}
		})
	}
}

func TestWrapperToolBridgeRecoveryRequiresServerProof(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/", nil)
	EnableWrapperStreamRecovery(c, []byte(`{"messages":[{"role":"user","content":"hello"}]}`))
	if _, err := AdvanceWrapperToolBridgeRecovery(c, &WrapperToolBridgeError{}); err == nil {
		t.Fatal("unsafe attempt retried")
	}
}
