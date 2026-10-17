package handler

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func jsonStream(events ...string) string {
	return "data: " + strings.Join(events, "\n\ndata: ") + "\n\n"
}

const jsonMessageStart = `{"type":"message_start","message":{"id":"msg_test","type":"message","role":"assistant","model":"opus","content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":2,"output_tokens":0,"cache_read_input_tokens":10000,"cache_creation_input_tokens":21}}}`

func TestWrapperJSONResponseToolsThinkingUsage(t *testing.T) {
	r := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(r)
	w := newWrapperJSONResponse(c.Writer)
	stream := jsonStream(jsonMessageStart,
		`{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","signature":""}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"consider"}}`,
		`{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"signed"}}`,
		`{"type":"content_block_stop","index":0}`,
		`{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"tool_1","name":"Read","input":{}}}`,
		`{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"path\":"}}`,
		`{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"\"/test\"}"}}`,
		`{"type":"content_block_stop","index":1}`,
		`{"type":"message_delta","delta":{"stop_reason":"tool_use","stop_sequence":null},"usage":{"input_tokens":4,"output_tokens":50,"cache_read_input_tokens":20000,"cache_creation_input_tokens":40}}`,
		`{"type":"message_stop","wrapper_turn_id":"private-receipt"}`)
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Content-Length", "999")
	for _, b := range []byte(stream) {
		_, err := w.Write([]byte{b})
		require.NoError(t, err)
		w.Flush()
	}
	require.Empty(t, r.Body.String())
	require.NoError(t, w.finish(context.Background()))
	require.Equal(t, "application/json", r.Header().Get("Content-Type"))
	require.Empty(t, r.Header().Get("Content-Length"))
	var msg map[string]any
	require.NoError(t, json.Unmarshal(r.Body.Bytes(), &msg))
	blocks := msg["content"].([]any)
	require.Len(t, blocks, 2)
	require.Equal(t, "signed", blocks[0].(map[string]any)["signature"])
	require.Equal(t, "/test", blocks[1].(map[string]any)["input"].(map[string]any)["path"])
	require.Equal(t, float64(50), msg["usage"].(map[string]any)["output_tokens"])
	require.NotContains(t, r.Body.String(), "private-receipt")
}

func TestWrapperJSONResponseRejectsPartialAndInvalidTool(t *testing.T) {
	for _, stream := range []string{
		jsonStream(jsonMessageStart),
		jsonStream(jsonMessageStart, `{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"t","name":"Edit","input":{}}}`, `{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{bad"}}`, `{"type":"content_block_stop","index":0}`, `{"type":"message_delta","delta":{"stop_reason":"tool_use"}}`, `{"type":"message_stop"}`),
		jsonStream(jsonMessageStart, jsonMessageStart),
	} {
		r := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(r)
		w := newWrapperJSONResponse(c.Writer)
		_, _ = w.WriteString(stream)
		require.Error(t, w.finish(context.Background()))
		require.Equal(t, 502, r.Code)
		require.NotContains(t, r.Body.String(), "tool_use")
	}
}

func TestWrapperJSONResponseErrorsAndCancel(t *testing.T) {
	for _, status := range []int{409, 429, 503} {
		r := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(r)
		w := newWrapperJSONResponse(c.Writer)
		w.WriteHeader(status)
		_, _ = w.WriteString(`{"type":"error","error":{"type":"session_busy"}}`)
		require.NoError(t, w.finish(context.Background()))
		require.Equal(t, status, r.Code)
	}
	r := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(r)
	w := newWrapperJSONResponse(c.Writer)
	_, _ = w.WriteString(jsonStream(jsonMessageStart, `{"type":"error","error":{"type":"rate_limit_error","message":"limited"}}`))
	require.NoError(t, w.finish(context.Background()))
	require.Equal(t, 429, r.Code)
	require.NotContains(t, r.Body.String(), "message_start")
	r = httptest.NewRecorder()
	c, _ = gin.CreateTestContext(r)
	w = newWrapperJSONResponse(c.Writer)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, w.finish(ctx), context.Canceled)
	c.Writer.WriteHeaderNow()
	require.Equal(t, 499, r.Code)
	require.Empty(t, r.Body.String())
}

func TestWrapperJSONResponseBounded(t *testing.T) {
	r := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(r)
	w := newWrapperJSONResponse(c.Writer)
	_, err := w.Write(make([]byte, wrapperJSONMaxBytes+1))
	require.Error(t, err)
	require.Empty(t, r.Body.String())
	require.Error(t, w.finish(context.Background()))
	require.Equal(t, 502, r.Code)
}

func TestWrapperJSONResponseRecoveredTextAndTerminalError(t *testing.T) {
	first := jsonStream(jsonMessageStart, `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":"before quota "}}`, `{"type":"content_block_stop","index":0}`)
	second := jsonStream(`{"type":"content_block_start","index":1,"content_block":{"type":"text","text":"after switch"}}`, `{"type":"content_block_stop","index":1}`, `{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"input_tokens":4,"output_tokens":17,"cache_read_input_tokens":12000,"cache_creation_input_tokens":80}}`, `{"type":"message_stop"}`)
	data, status, err := wrapperSSEToJSON([]byte(first + second))
	require.NoError(t, err)
	require.Equal(t, 200, status)
	var msg map[string]any
	require.NoError(t, json.Unmarshal(data, &msg))
	require.Len(t, msg["content"], 2)
	require.Equal(t, float64(17), msg["usage"].(map[string]any)["output_tokens"])
	data, status, err = wrapperSSEToJSON([]byte(first + jsonStream(`{"type":"error","error":{"type":"rate_limit_error","message":"Recovery exhausted"}}`)))
	require.NoError(t, err)
	require.Equal(t, 429, status)
	require.NotContains(t, string(data), "before quota")
}
