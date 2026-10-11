//go:build unit

package service

import (
	"context"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestNormalizeWrapperMessages(t *testing.T) {
	body := []byte(`{"model":"claude-opus-5-5","messages":[{"role":"system","content":[],"output_config":{"effort":"high"}},{"role":"user","content":"hi"},{"role":"system","content":[{"type":"text","text":"follow the project rules"}],"output_config":{"effort":"high"}},{"role":"assistant","content":[{"type":"text","text":"ok"}]}]}`)
	out, err := normalizeWrapperMessages(body)
	require.NoError(t, err)
	messages := gjson.GetBytes(out, "messages").Array()
	require.Len(t, messages, 3)
	require.Equal(t, "user", messages[0].Get("role").String())
	require.Equal(t, "hi", messages[0].Get("content").String())
	require.Equal(t, "user", messages[1].Get("role").String())
	require.Contains(t, messages[1].Get("content").String(), "follow the project rules")
	require.Equal(t, "assistant", messages[2].Get("role").String())
	require.Equal(t, "claude-opus-5-5", gjson.GetBytes(out, "model").String())
}

func TestWrapperMessageNormalizationIsAccountScoped(t *testing.T) {
	svc := &GatewayService{cfg: &config.Config{Security: config.SecurityConfig{URLAllowlist: config.URLAllowlistConfig{Enabled: false, AllowInsecureHTTP: true}}}}
	body := []byte(`{"model":"claude-opus-5-5","max_tokens":100,"messages":[{"role":"system","content":[]},{"role":"user","content":"hi"}]}`)
	wrapper := newAnthropicAPIKeyAccountForTest()
	wrapper.Extra["wrapper_protocol"] = "anthropic-messages-cli-v1"
	_, wrapperBody, err := svc.buildUpstreamRequestAnthropicAPIKeyPassthrough(context.Background(), nil, wrapper, body, "upstream-key")
	require.NoError(t, err)
	require.Len(t, gjson.GetBytes(wrapperBody, "messages").Array(), 1)
	ordinary := newAnthropicAPIKeyAccountForTest()
	_, ordinaryBody, err := svc.buildUpstreamRequestAnthropicAPIKeyPassthrough(context.Background(), nil, ordinary, body, "upstream-key")
	require.NoError(t, err)
	require.Len(t, gjson.GetBytes(ordinaryBody, "messages").Array(), 2)
}

func TestNormalizeWrapperMessagesRejectsNonTextSystem(t *testing.T) {
	_, err := normalizeWrapperMessages([]byte(`{"messages":[{"role":"system","content":[{"type":"image","source":{}}]},{"role":"user","content":"hi"}]}`))
	require.ErrorContains(t, err, "non-text system")
}
