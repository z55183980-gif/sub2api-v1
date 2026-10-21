package service

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestOpus55LegacyThinkingCompatibility(t *testing.T) {
	for _, tc := range []struct{ name, model, controls, wantType, wantEffort string }{
		{"disabled", "claude-opus-5-5", `"thinking":{"type":"disabled"}`, "adaptive", "low"},
		{"disabled overrides effort", "claude-opus-5-5", `"thinking":{"type":"disabled","budget_tokens":4096},"output_config":{"effort":"max","format":{"type":"json_schema"}}`, "adaptive", "low"},
		{"enabled keeps effort", "claude-opus-5-5", `"thinking":{"type":"enabled","budget_tokens":4096,"display":"omitted"},"output_config":{"effort":"xhigh"}`, "adaptive", "xhigh"},
		{"enabled keeps default", "claude-opus-5-5", `"thinking":{"type":"enabled","budget_tokens":4096}`, "adaptive", ""},
		{"adaptive drops stale budget", "claude-opus-5-5", `"thinking":{"type":"adaptive","budget_tokens":4096}`, "adaptive", ""},
		{"omitted", "claude-opus-5-5", `"max_tokens":128`, "", ""},
		{"other model untouched", "claude-opus-5", `"thinking":{"type":"disabled"},"output_config":{"effort":"high"}`, "disabled", "high"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(`{"model":"` + tc.model + `",` + tc.controls + `,"messages":[{"role":"assistant","content":[{"type":"thinking","thinking":"","signature":"signed"}]},{"role":"user","content":"hi"}]}`)
			before := string(body)
			got, err := prepareClaudeOpus55Request(body, tc.model)
			require.NoError(t, err)
			require.Equal(t, before, string(body), "must not mutate a shared request buffer")
			require.Equal(t, tc.wantType, gjson.GetBytes(got, "thinking.type").String())
			require.Equal(t, tc.wantEffort, gjson.GetBytes(got, "output_config.effort").String())
			require.Equal(t, gjson.GetBytes(body, "messages").Raw, gjson.GetBytes(got, "messages").Raw)
			require.Equal(t, gjson.GetBytes(body, "thinking.display").Raw, gjson.GetBytes(got, "thinking.display").Raw)
			require.Equal(t, gjson.GetBytes(body, "output_config.format").Raw, gjson.GetBytes(got, "output_config.format").Raw)
			require.False(t, gjson.GetBytes(got, "thinking.budget_tokens").Exists())
			again, err := prepareClaudeOpus55Request(got, tc.model)
			require.NoError(t, err)
			require.Equal(t, string(got), string(again))
			if tc.model != "claude-opus-5-5" {
				require.Equal(t, before, string(got))
			}
		})
	}
}

func TestOpus55LegacyThinkingReachesMessagesAndCountUpstream(t *testing.T) {
	for _, count := range []bool{false, true} {
		for _, mode := range []string{"disabled", "enabled"} {
			name := mode + "/messages"
			if count {
				name = mode + "/count_tokens"
			}
			t.Run(name, func(t *testing.T) {
				body := []byte(`{"model":"public-opus","max_tokens":128,"thinking":{"type":"` + mode + `","budget_tokens":4096},"output_config":{"effort":"high"},"messages":[{"role":"user","content":"hello"}]}`)
				parsed, err := ParseGatewayRequest(NewRequestBodyRef(body), PlatformAnthropic)
				require.NoError(t, err)
				payload := `{"id":"msg_test","model":"claude-opus-5-5","type":"message","role":"assistant","content":[{"type":"text","text":"ok"}],"stop_reason":"end_turn","usage":{"input_tokens":1,"output_tokens":1}}`
				if count {
					payload = `{"input_tokens":42}`
				}
				upstream := &anthropicHTTPUpstreamRecorder{resp: &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(payload))}}
				svc := &GatewayService{cfg: &config.Config{}, httpUpstream: upstream, rateLimitService: &RateLimitService{}}
				account := newAnthropicAPIKeyAccountForTest()
				account.Credentials["model_mapping"] = map[string]any{"public-opus": "claude-opus-5-5"}
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", strings.NewReader(string(body)))
				if count {
					err = svc.ForwardCountTokens(context.Background(), c, account, parsed)
				} else {
					_, err = svc.Forward(context.Background(), c, account, parsed)
				}
				require.NoError(t, err)
				require.Equal(t, 200, rec.Code)
				require.NotNil(t, upstream.lastReq)
				require.Equal(t, "adaptive", gjson.GetBytes(upstream.lastBody, "thinking.type").String())
				require.False(t, gjson.GetBytes(upstream.lastBody, "thinking.budget_tokens").Exists())
				effort := "high"
				if mode == "disabled" {
					effort = "low"
				}
				require.Equal(t, effort, parsed.OutputEffort)
				require.True(t, parsed.ThinkingEnabled)
				if !count {
					require.Equal(t, effort, gjson.GetBytes(upstream.lastBody, "output_config.effort").String())
				}
			})
		}
	}
}
