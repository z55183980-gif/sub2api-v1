package service

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const wrapperRecoveryContextKey = "wrapper_stream_recovery"
const wrapperRecoveryMaxBytes = 16 << 20

// WrapperStreamRecovery owns the single client response across account attempts.
// It is request-local; it never accepts state or instructions from HTTP headers.
type WrapperStreamRecovery struct {
	original         []byte
	started          bool
	nextIndex        int
	text             string
	blocks           []json.RawMessage
	usage            ClaudeUsage
	switches         int
	deadline         time.Time
	committed        *WrapperManagedAutoTurn
	bridgeRetries    int
	contentPublished bool
}

type WrapperToolBridgeError struct{ RetrySafe bool }

// Partial text cannot be retracted or replayed. Keep this distinct from quota
// and bridge errors, whose existing recovery paths may retry.
type WrapperPartialResponseError struct{}

func (e *WrapperPartialResponseError) Error() string {
	return "Response stopped after partial text; reset the session. The request was not replayed."
}

func (e *WrapperToolBridgeError) Error() string { return "client_tool_bridge_violation" }

// Recover only a fresh user turn. A pending tool result needs its live MCP waiter.
func AdvanceWrapperToolBridgeRecovery(c *gin.Context, failure *WrapperToolBridgeError) ([]byte, error) {
	r := wrapperRecovery(c)
	if r == nil || !failure.RetrySafe || r.contentPublished || r.bridgeRetries > 0 || r.switches > 0 {
		return nil, errors.New("Client tool bridge failed; automatic replay is unsafe or exhausted")
	}
	var messages []json.RawMessage
	if err := json.Unmarshal([]byte(gjson.GetBytes(r.original, "messages").Raw), &messages); err != nil {
		return nil, err
	}
	if !validManagedBootstrapHistory(messages) {
		return nil, errors.New("Client tool bridge failed during a tool continuation; retry was not performed")
	}
	r.bridgeRetries++
	r.nextIndex = 0
	return append([]byte(nil), r.original...), nil
}

type WrapperStreamLimitError struct{ Body []byte }

func (e *WrapperStreamLimitError) Error() string { return "wrapper quota exhausted during stream" }

func EnableWrapperStreamRecovery(c *gin.Context, body []byte) {
	if normalized, err := normalizedWrapperManagedBody(body, c.GetHeader("anthropic-beta")); err == nil {
		body = normalized
	}
	c.Set(wrapperRecoveryContextKey, &WrapperStreamRecovery{original: append([]byte(nil), body...)})
}
func wrapperRecovery(c *gin.Context) *WrapperStreamRecovery {
	v, _ := c.Get(wrapperRecoveryContextKey)
	r, _ := v.(*WrapperStreamRecovery)
	return r
}

func WrapperStreamRecoveryActive(c *gin.Context) bool {
	r := wrapperRecovery(c)
	return r != nil && r.switches > 0
}

func WrapperStreamRecoveryEnabled(c *gin.Context) bool { return wrapperRecovery(c) != nil }

// Advance is called only for the typed quota error, after recording that attempt's usage.
func AdvanceWrapperStreamRecovery(c *gin.Context) ([]byte, error) {
	r := wrapperRecovery(c)
	if r == nil {
		return nil, errors.New("wrapper recovery unavailable")
	}
	if r.deadline.IsZero() {
		r.deadline = time.Now().Add(120 * time.Second)
		ctx, cancel := context.WithDeadline(c.Request.Context(), r.deadline)
		c.Request = c.Request.WithContext(ctx)
		c.Set(wrapperRecoveryContextKey+"_cancel", cancel)
	}
	if r.switches >= 3 || time.Now().After(r.deadline) {
		return nil, errors.New("wrapper recovery budget exhausted")
	}
	r.switches++
	var messages []json.RawMessage
	if err := json.Unmarshal([]byte(gjson.GetBytes(r.original, "messages").Raw), &messages); err != nil {
		return nil, err
	}
	if r.text != "" {
		b, _ := json.Marshal(map[string]any{"role": "assistant", "content": r.text})
		messages = append(messages, b)
	}
	// Completed tool results are history, not a new invocation on the replacement CLI.
	if r.text != "" || (len(messages) > 0 && hasManagedToolResult(messages[len(messages)-1])) {
		b, _ := json.Marshal(map[string]any{"role": "user", "content": "Continue the interrupted response from exactly where the assistant stopped. Do not repeat the text already shown. Treat all tool results in the history as completed; do not repeat completed operations. Finish the remaining response or request only new necessary tools."})
		messages = append(messages, b)
	}
	return sjson.SetBytes(r.original, "messages", messages)
}

func FinishWrapperStreamRecovery(c *gin.Context) {
	if v, ok := c.Get(wrapperRecoveryContextKey + "_cancel"); ok {
		v.(context.CancelFunc)()
	}
}

func (r *WrapperStreamRecovery) checkDeadline() error {
	if !r.deadline.IsZero() && time.Now().After(r.deadline) {
		return errors.New("wrapper recovery exceeded 120 seconds")
	}
	return nil
}

func addWrapperUsage(dst *ClaudeUsage, src *ClaudeUsage) {
	dst.InputTokens += src.InputTokens
	dst.OutputTokens += src.OutputTokens
	dst.CacheCreationInputTokens += src.CacheCreationInputTokens
	dst.CacheReadInputTokens += src.CacheReadInputTokens
	dst.CacheCreation5mTokens += src.CacheCreation5mTokens
	dst.CacheCreation1hTokens += src.CacheCreation1hTokens
}

// A resumed native transcript includes an internal continuation prompt. Keep the
// client proof separate so the next real tool result can acknowledge its receipt.
func (t *WrapperManagedAutoTurn) rebaseRecoveryClientHistory(r *WrapperStreamRecovery) error {
	if (r.switches == 0 && r.bridgeRetries == 0) || !t.IsReady() {
		return nil
	}
	var messages []json.RawMessage
	if err := json.Unmarshal([]byte(gjson.GetBytes(r.original, "messages").Raw), &messages); err != nil {
		return err
	}
	assistant, _ := json.Marshal(map[string]any{"role": "assistant", "content": r.blocks})
	messages = append(messages, assistant)
	t.state.Messages = nil // proofs avoid retaining a second full transcript in Redis
	t.state.Proofs = managedProofs(messages)
	t.state.Recent = managedRecent(messages)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return t.save(ctx)
}

// Only removes an exact suffix/prefix overlap, bounded to 2 KiB. Ambiguous short
// fragments are retained instead of guessing and deleting legitimate text.
func wrapperContinuationPrefix(previous, next string) string {
	limit := min(len(previous), len(next), 2048)
	for n := limit; n >= 16; n-- {
		if strings.HasSuffix(previous, next[:n]) {
			return next[n:]
		}
	}
	return next
}

func (s *GatewayService) streamWrapperWithRecovery(ctx context.Context, resp *http.Response, c *gin.Context, account *Account, start time.Time, turn *WrapperManagedAutoTurn, r *WrapperStreamRecovery) (out *streamingResult, streamErr error) {
	defer resp.Body.Close()
	observer := upstreamResponseModelObserverFromContext(c)
	if observer == nil {
		observer = beginUpstreamResponseModelObservation(c)
	}
	if s.rateLimitService != nil {
		s.rateLimitService.UpdateSessionWindow(ctx, account, resp.Header)
	}
	usage := &ClaudeUsage{}
	result := &streamingResult{usage: usage}
	defer func() { addWrapperUsage(&r.usage, usage) }()
	c.Header("Content-Type", "text/event-stream")
	c.Header("Cache-Control", "no-cache")
	c.Header("X-Accel-Buffering", "no")
	if !r.started {
		c.Header("x-request-id", resp.Header.Get("x-request-id"))
	}
	write := func(data []byte) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		event := gjson.GetBytes(data, "type").String()
		_, err := fmt.Fprintf(c.Writer, "event: %s\ndata: %s\n\n", event, data)
		c.Writer.Flush()
		return err
	}
	type frame struct {
		data []byte
		err  error
	}
	frames := make(chan frame, 1)
	done := make(chan struct{})
	defer close(done)
	go func() {
		defer close(frames)
		scan := bufio.NewScanner(resp.Body)
		scan.Buffer(make([]byte, 65536), wrapperRecoveryMaxBytes)
		var data []string
		size := 0
		send := func(f frame) bool {
			select {
			case frames <- f:
				return true
			case <-done:
				return false
			}
		}
		for scan.Scan() {
			line := scan.Text()
			if line == "" {
				if len(data) > 0 {
					if !send(frame{data: []byte(strings.Join(data, "\n"))}) {
						return
					}
				}
				data = nil
				size = 0
				continue
			}
			if strings.HasPrefix(line, "data:") {
				v := strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " ")
				size += len(v)
				if size > wrapperRecoveryMaxBytes {
					send(frame{err: errors.New("wrapper SSE frame too large")})
					return
				}
				data = append(data, v)
			}
		}
		if len(data) > 0 {
			if !send(frame{data: []byte(strings.Join(data, "\n"))}) {
				return
			}
		}
		if err := scan.Err(); err != nil {
			send(frame{err: err})
		}
	}()
	heartbeat := time.NewTicker(10 * time.Second)
	defer heartbeat.Stop()
	lastData := time.Now()
	indices := map[int]int{}
	kinds := map[int]string{}
	clientBlocks := &WrapperManagedAutoTurn{blocks: map[int]json.RawMessage{}, toolJSON: map[int]string{}}
	firstIndex := r.nextIndex
	open := map[int]bool{}
	pending := [][]byte{}
	pendingBytes := 0
	buffering := false
	toolPending := false
	var terminalDelta []byte
	prefix := ""
	prefixIndex := -1
	prefixDone := r.switches == 0
	priorText := r.text
	publish := func(data []byte) error {
		typ := gjson.GetBytes(data, "type").String()
		if strings.HasPrefix(typ, "content_block_") {
			r.contentPublished = true
		}
		idx := int(gjson.GetBytes(data, "index").Int())
		if typ == "content_block_start" {
			open[idx] = true
		}
		if typ == "content_block_stop" {
			delete(open, idx)
		}
		if err := write(data); err != nil {
			return err
		}
		clientBlocks.Observe(data)
		if typ == "content_block_delta" && gjson.GetBytes(data, "delta.type").String() == "text_delta" {
			r.text += gjson.GetBytes(data, "delta.text").String()
		}
		if typ == "content_block_start" && gjson.GetBytes(data, "content_block.type").String() == "text" {
			r.text += gjson.GetBytes(data, "content_block.text").String()
		}
		if len(r.text) > wrapperRecoveryMaxBytes {
			return errors.New("wrapper continuation text too large")
		}
		return nil
	}
	flushPrefix := func() error {
		if prefix == "" {
			prefixDone = true
			return nil
		}
		value := wrapperContinuationPrefix(priorText, prefix)
		prefix = ""
		prefixDone = true
		if value == "" {
			return nil
		}
		b, _ := json.Marshal(map[string]any{"type": "content_block_delta", "index": prefixIndex, "delta": map[string]any{"type": "text_delta", "text": value}})
		return publish(b)
	}
	finishBlocks := func() {
		for idx := firstIndex; idx < r.nextIndex; idx++ {
			if b, ok := clientBlocks.blocks[idx]; ok && gjson.GetBytes(b, "type").String() != "thinking" {
				r.blocks = append(r.blocks, append(json.RawMessage(nil), b...))
			}
		}
	}
	for {
		select {
		case <-ctx.Done():
			result.clientDisconnect = true
			return result, ctx.Err()
		case <-heartbeat.C:
			if err := r.checkDeadline(); err != nil {
				return result, err
			}
			if time.Since(lastData) > 120*time.Second {
				return result, errors.New("wrapper stream idle timeout")
			}
			if _, err := io.WriteString(c.Writer, ": ping\n\n"); err != nil {
				return result, err
			}
			c.Writer.Flush()
		case f, ok := <-frames:
			if !ok {
				return result, errors.New("wrapper stream ended without message_stop")
			}
			if f.err != nil {
				return result, f.err
			}
			lastData = time.Now()
			if err := r.checkDeadline(); err != nil {
				return result, err
			}
			if !gjson.ValidBytes(f.data) {
				return result, errors.New("invalid wrapper SSE JSON")
			}
			data := reverseToolNamesIfPresent(c, f.data)
			observer.ObserveAnthropic(data)
			typ := gjson.GetBytes(data, "type").String()
			parseSSEUsagePassthrough(string(data), usage)
			if typ == "message_start" {
				usage.OutputTokens = int(gjson.GetBytes(data, "message.usage.output_tokens").Int())
			}
			if result.firstTokenMs == nil && typ == "content_block_delta" &&
				(gjson.GetBytes(data, "delta.type").String() == "text_delta" || gjson.GetBytes(data, "delta.type").String() == "input_json_delta") {
				ms := int(time.Since(start).Milliseconds())
				result.firstTokenMs = &ms
			}
			if typ == "error" {
				if gjson.GetBytes(data, "error.type").String() == "managed_partial_response" {
					if gjson.GetBytes(data, "error.reason").String() == "late_rate_limit" && s.rateLimitService != nil {
						stateCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
						s.rateLimitService.handleWrapperRateLimit(stateCtx, account, resp.Header, data)
						cancel()
					}
					if confirmed := gjson.GetBytes(data, "error.usage"); confirmed.IsObject() {
						parseSSEUsagePassthrough(`{"type":"message_delta","usage":`+confirmed.Raw+`}`, usage)
					}
					return result, &WrapperPartialResponseError{}
				}
				if gjson.GetBytes(data, "error.type").String() == "client_tool_bridge_violation" {
					return result, &WrapperToolBridgeError{RetrySafe: gjson.GetBytes(data, "error.retry_safe").Bool() && !r.contentPublished}
				}
				if gjson.GetBytes(data, "error.type").String() != "rate_limit_error" {
					return result, fmt.Errorf("wrapper stream reported error: %s", gjson.GetBytes(data, "error.type").String())
				}
				stateCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
				s.rateLimitService.handleWrapperRateLimit(stateCtx, account, resp.Header, data)
				cancel()
				if err := flushPrefix(); err != nil {
					return result, err
				}
				for idx := range open {
					b, _ := json.Marshal(map[string]any{"type": "content_block_stop", "index": idx})
					if err := write(b); err != nil {
						return result, err
					}
				}
				finishBlocks()
				// Buffered tools were never published; discard them and reuse indices.
				for _, b := range pending {
					if gjson.GetBytes(b, "type").String() == "content_block_start" {
						idx := int(gjson.GetBytes(b, "index").Int())
						if idx < r.nextIndex {
							r.nextIndex = idx
						}
						break
					}
				}
				return result, &WrapperStreamLimitError{Body: append([]byte(nil), data...)}
			}
			if turn != nil {
				turn.Observe(data)
			}
			if typ == "message_start" {
				if r.started {
					continue
				}
				r.started = true
				if err := write(data); err != nil {
					return result, err
				}
				continue
			}
			if typ == "content_block_start" {
				old := int(gjson.GetBytes(data, "index").Int())
				idx := r.nextIndex
				r.nextIndex++
				indices[old] = idx
				kind := gjson.GetBytes(data, "content_block.type").String()
				kinds[idx] = kind
				data, _ = sjson.SetBytes(data, "index", idx)
				if kind != "text" {
					buffering = true
					if kind != "thinking" && kind != "redacted_thinking" {
						toolPending = true
					}
				}
				if kind == "text" && !buffering {
					initial := gjson.GetBytes(data, "content_block.text").String()
					data, _ = sjson.SetBytes(data, "content_block.text", "")
					if err := publish(data); err != nil {
						return result, err
					}
					if initial != "" {
						b, _ := json.Marshal(map[string]any{"type": "content_block_delta", "index": idx, "delta": map[string]any{"type": "text_delta", "text": initial}})
						data = b
						typ = "content_block_delta"
					} else {
						continue
					}
				}
			} else if strings.HasPrefix(typ, "content_block_") {
				old := int(gjson.GetBytes(data, "index").Int())
				idx, exists := indices[old]
				if !exists {
					return result, errors.New("wrapper block index missing")
				}
				data, _ = sjson.SetBytes(data, "index", idx)
			}
			if typ == "content_block_delta" && !buffering && gjson.GetBytes(data, "delta.type").String() == "text_delta" {
				idx := int(gjson.GetBytes(data, "index").Int())
				value := gjson.GetBytes(data, "delta.text").String()
				if !prefixDone {
					prefixIndex = idx
					prefix += value
					if len(prefix) < 2048 {
						continue
					}
					if err := flushPrefix(); err != nil {
						return result, err
					}
					continue
				}
			}
			if typ == "content_block_stop" && !buffering && !prefixDone {
				if err := flushPrefix(); err != nil {
					return result, err
				}
			}
			if typ == "message_delta" {
				terminalDelta = append([]byte(nil), data...)
				continue
			}
			if typ == "message_stop" {
				if terminalDelta == nil {
					return result, errors.New("wrapper terminal usage missing")
				}
				if err := flushPrefix(); err != nil {
					return result, err
				}
				if turn == nil {
					return result, errors.New("wrapper terminal receipt invalid")
				}
				for _, b := range pending {
					clientBlocks.Observe(b)
				}
				finishBlocks()
				commitCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
				err := turn.Commit(commitCtx)
				cancel()
				if err == nil {
					err = turn.rebaseRecoveryClientHistory(r)
				}
				if err != nil {
					return result, err
				}
				r.committed = turn
				for _, b := range pending {
					if err := write(b); err != nil {
						return result, err
					}
				}
				total := r.usage
				addWrapperUsage(&total, usage)
				for key, value := range map[string]int{"input_tokens": total.InputTokens, "output_tokens": total.OutputTokens, "cache_creation_input_tokens": total.CacheCreationInputTokens, "cache_read_input_tokens": total.CacheReadInputTokens} {
					terminalDelta, _ = sjson.SetBytes(terminalDelta, "usage."+key, value)
				}
				if err := write(terminalDelta); err != nil {
					return result, err
				}
				if err := write(data); err != nil {
					return result, err
				}
				return result, nil
			}
			if buffering {
				pendingBytes += len(data)
				if pendingBytes > wrapperRecoveryMaxBytes {
					return result, errors.New("wrapper tool buffer too large")
				}
				pending = append(pending, append([]byte(nil), data...))
				// A complete thinking block cannot execute a tool. Release it so
				// subsequent text still streams while tool blocks remain atomic.
				if !toolPending && typ == "content_block_stop" && (kinds[int(gjson.GetBytes(data, "index").Int())] == "thinking" || kinds[int(gjson.GetBytes(data, "index").Int())] == "redacted_thinking") {
					for _, b := range pending {
						if err := publish(b); err != nil {
							return result, err
						}
					}
					pending = nil
					pendingBytes = 0
					buffering = false
				}
				continue
			}
			if err := publish(data); err != nil {
				return result, err
			}
		}
	}
}
