//go:build unit

package service

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
)

func TestAccountTestService_WrapperProbeOmitsTemperature(t *testing.T) {
	for _, tc := range []struct {
		name            string
		wrapperProtocol string
		wantTemperature bool
	}{
		{name: "wrapper", wrapperProtocol: "anthropic-messages-cli-v1"},
		{name: "ordinary Anthropic API key", wantTemperature: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			account := &Account{
				ID:          701,
				Name:        tc.name,
				Platform:    PlatformAnthropic,
				Type:        AccountTypeAPIKey,
				Status:      StatusActive,
				Concurrency: 1,
				Credentials: map[string]any{
					"api_key":  "test-key",
					"base_url": "https://api.anthropic.com",
				},
				Extra: map[string]any{"wrapper_protocol": tc.wrapperProtocol},
			}
			upstream := &queuedHTTPUpstream{responses: []*http.Response{{
				StatusCode: http.StatusOK,
				Header:     http.Header{"Content-Type": []string{"text/event-stream"}},
				Body:       io.NopCloser(strings.NewReader("data: {\"type\":\"content_block_delta\",\"delta\":{\"text\":\"ok\"}\n\n")),
			}}}
			repo := &openAIAccountTestRepo{mockAccountRepoForGemini: mockAccountRepoForGemini{
				accountsByID: map[int64]*Account{account.ID: account},
			}}
			svc := &AccountTestService{
				accountRepo:  repo,
				httpUpstream: upstream,
				cfg:          rawChatCompletionsTestConfig(),
			}
			c, _ := newTestContext()
			require.NoError(t, svc.TestAccountConnection(c, account.ID, "claude-sonnet-5", "hi", AccountTestModeDefault))
			require.Len(t, upstream.requests, 1)
			body, err := io.ReadAll(upstream.requests[0].Body)
			require.NoError(t, err)
			require.Equal(t, tc.wantTemperature, gjson.GetBytes(body, "temperature").Exists())
			require.Equal(t, "claude-sonnet-5", gjson.GetBytes(body, "model").String())
		})
	}
}
