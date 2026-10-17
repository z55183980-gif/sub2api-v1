package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/sjson"
)

const wrapperJSONMaxBytes = 32 << 20

func wrapperStreamingBody(body []byte) ([]byte, error) { return sjson.SetBytes(body, "stream", true) }

// Hold the internal stream, including heartbeats and recovery attempts, until a
// complete message is available. Nothing is flushed to the JSON client early.
type wrapperJSONResponse struct {
	gin.ResponseWriter
	buf     bytes.Buffer
	header  http.Header
	status  int
	written bool
	err     error
	persist func([]byte) error
	durable bool
}

func newWrapperJSONResponse(w gin.ResponseWriter) *wrapperJSONResponse {
	return &wrapperJSONResponse{ResponseWriter: w, header: w.Header().Clone(), status: 200}
}
func (w *wrapperJSONResponse) Header() http.Header { return w.header }
func (w *wrapperJSONResponse) WriteHeader(status int) {
	if !w.written {
		w.status = status
	}
}
func (w *wrapperJSONResponse) WriteHeaderNow() { w.written = true }
func (w *wrapperJSONResponse) Written() bool   { return w.written }
func (w *wrapperJSONResponse) Status() int     { return w.status }
func (w *wrapperJSONResponse) Size() int {
	if !w.written {
		return -1
	}
	return w.buf.Len()
}
func (w *wrapperJSONResponse) Flush()                            { w.written = true }
func (w *wrapperJSONResponse) WriteString(s string) (int, error) { return w.Write([]byte(s)) }
func (w *wrapperJSONResponse) Write(b []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}
	if w.buf.Len()+len(b) > wrapperJSONMaxBytes {
		w.err = errors.New("managed JSON response exceeds buffer limit")
		return 0, w.err
	}
	w.written = true
	return w.buf.Write(b)
}

func (w *wrapperJSONResponse) finish(ctx context.Context) error {
	if ctx.Err() != nil {
		if !w.ResponseWriter.Written() {
			w.ResponseWriter.WriteHeader(499)
		}
		return ctx.Err()
	}
	status := w.status
	body := w.buf.Bytes()
	err := w.err
	if err == nil && status < 400 {
		body, status, err = wrapperSSEToJSON(body)
	}
	if err != nil && status < 400 {
		status = 502
	}
	if err != nil {
		body, _ = json.Marshal(map[string]any{"type": "error", "error": map[string]string{"type": "api_error", "message": "Managed upstream response did not complete safely"}})
	}
	if err == nil && status == http.StatusOK && w.persist != nil {
		if persistErr := w.persist(body); persistErr != nil {
			err = persistErr
			status = http.StatusServiceUnavailable
			body, _ = json.Marshal(map[string]any{"type": "error", "error": map[string]string{"type": "api_error", "message": "Unable to retain complete response for safe retry"}})
		} else {
			w.durable = true
		}
	}
	for key, values := range w.header {
		w.ResponseWriter.Header()[key] = values
	}
	for _, key := range []string{"Content-Length", "Content-Encoding", "Transfer-Encoding", "X-Accel-Buffering"} {
		w.ResponseWriter.Header().Del(key)
	}
	w.ResponseWriter.Header().Set("Content-Type", "application/json")
	w.ResponseWriter.WriteHeader(status)
	_, writeErr := w.ResponseWriter.Write(body)
	if err != nil {
		return err
	}
	return writeErr
}

func wrapperSSEToJSON(raw []byte) ([]byte, int, error) {
	var message map[string]any
	blocks := map[int]map[string]any{}
	inputs := map[int]string{}
	stopped := map[int]bool{}
	terminal := false
	for _, frame := range strings.Split(strings.ReplaceAll(string(raw), "\r\n", "\n"), "\n\n") {
		var data []string
		for _, line := range strings.Split(frame, "\n") {
			if strings.HasPrefix(line, "data:") {
				data = append(data, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
			}
		}
		if len(data) == 0 {
			continue
		}
		var e map[string]any
		if err := json.Unmarshal([]byte(strings.Join(data, "\n")), &e); err != nil {
			return nil, 502, err
		}
		typ, _ := e["type"].(string)
		idxFloat, _ := e["index"].(float64)
		idx := int(idxFloat)
		if terminal && typ != "ping" {
			return nil, 502, errors.New("event after message_stop")
		}
		switch typ {
		case "ping":
		case "error":
			status := 502
			detail, _ := e["error"].(map[string]any)
			switch detail["type"] {
			case "rate_limit_error":
				status = 429
			case "session_busy", "session_state_error":
				status = 409
			case "invalid_request_error":
				status = 400
			}
			b, err := json.Marshal(e)
			return b, status, err
		case "message_start":
			if message != nil {
				return nil, 502, errors.New("duplicate message_start")
			}
			message, _ = e["message"].(map[string]any)
			if message == nil {
				return nil, 502, errors.New("missing message")
			}
		case "content_block_start":
			if message == nil || blocks[idx] != nil || idx < 0 || idx > 4096 {
				return nil, 502, errors.New("invalid block index")
			}
			b, ok := e["content_block"].(map[string]any)
			if !ok {
				return nil, 502, errors.New("missing block")
			}
			blocks[idx] = b
		case "content_block_delta":
			b := blocks[idx]
			d, _ := e["delta"].(map[string]any)
			if b == nil || stopped[idx] || d == nil {
				return nil, 502, errors.New("invalid block delta")
			}
			field := ""
			switch d["type"] {
			case "text_delta":
				field = "text"
			case "thinking_delta":
				field = "thinking"
			case "signature_delta":
				field = "signature"
			case "input_json_delta":
				v, _ := d["partial_json"].(string)
				inputs[idx] += v
				continue
			default:
				return nil, 502, fmt.Errorf("unsupported block delta %v", d["type"])
			}
			v, _ := d[field].(string)
			prev, _ := b[field].(string)
			b[field] = prev + v
		case "content_block_stop":
			if blocks[idx] == nil || stopped[idx] {
				return nil, 502, errors.New("invalid block stop")
			}
			stopped[idx] = true
		case "message_delta":
			if message == nil {
				return nil, 502, errors.New("delta before start")
			}
			if d, ok := e["delta"].(map[string]any); ok {
				for k, v := range d {
					message[k] = v
				}
			}
			if u, ok := e["usage"].(map[string]any); ok {
				old, _ := message["usage"].(map[string]any)
				if old == nil {
					old = map[string]any{}
				}
				for k, v := range u {
					old[k] = v
				}
				message["usage"] = old
			}
		case "message_stop":
			terminal = true
		default:
			return nil, 502, fmt.Errorf("unsupported SSE event %s", typ)
		}
	}
	if !terminal || message == nil || message["stop_reason"] == nil {
		return nil, 502, errors.New("incomplete message")
	}
	content := make([]any, 0, len(blocks))
	for i := 0; i < len(blocks); i++ {
		b := blocks[i]
		if b == nil || !stopped[i] {
			return nil, 502, errors.New("incomplete content block")
		}
		if s, ok := inputs[i]; ok {
			var input map[string]any
			if json.Unmarshal([]byte(s), &input) != nil || input == nil {
				return nil, 502, errors.New("invalid tool input")
			}
			b["input"] = input
		}
		content = append(content, b)
	}
	message["content"] = content
	b, err := json.Marshal(message)
	return b, 200, err
}
