package handler

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

type disconnectedJSONWriter struct{ gin.ResponseWriter }

func TestWrapperPartialResponseNeverPersistsSuccessfulJSONReplay(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	w := newWrapperJSONResponse(c.Writer)
	stored := false
	w.persist = func([]byte) error { stored = true; return nil }
	_, _ = w.Write([]byte(jsonStream(jsonMessageStart, `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":"partial"}}`, `{"type":"content_block_stop","index":0}`, `{"type":"error","error":{"type":"managed_partial_response","message":"Session reset required; request was not replayed"}}`)))
	_ = w.finish(context.Background())
	if stored || w.durable || recorder.Code == 200 || !strings.Contains(recorder.Body.String(), "error") {
		t.Fatalf("partial response incorrectly cached: status=%d stored=%v body=%s", recorder.Code, stored, recorder.Body.String())
	}
}

func (w disconnectedJSONWriter) Write([]byte) (int, error) {
	return 0, errors.New("client disconnected")
}

func TestWrapperJSONDurableResultSurvivesClientWriteFailure(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	w := newWrapperJSONResponse(disconnectedJSONWriter{c.Writer})
	stored := false
	w.persist = func(body []byte) error { stored = len(body) > 0; return nil }
	_, _ = w.Write([]byte(jsonStream(jsonMessageStart, `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":"ok"}}`, `{"type":"content_block_stop","index":0}`, `{"type":"message_delta","delta":{"stop_reason":"end_turn"}}`, `{"type":"message_stop"}`)))
	if e := w.finish(context.Background()); e == nil || !stored || !w.durable {
		t.Fatalf("lost complete response on disconnect: %v", e)
	}
}

func TestWrapperJSONPersistenceFailureReturnsErrorResponse(t *testing.T) {
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	w := newWrapperJSONResponse(c.Writer)
	w.persist = func([]byte) error { return errors.New("store unavailable") }
	_, _ = w.Write([]byte(jsonStream(jsonMessageStart, `{"type":"content_block_start","index":0,"content_block":{"type":"text","text":"ok"}}`, `{"type":"content_block_stop","index":0}`, `{"type":"message_delta","delta":{"stop_reason":"end_turn"}}`, `{"type":"message_stop"}`)))
	if e := w.finish(context.Background()); e == nil || w.durable || recorder.Code != 503 || recorder.Body.Len() == 0 {
		t.Fatalf("persistence failure must not return an empty success: %v status=%d", e, recorder.Code)
	}
}
