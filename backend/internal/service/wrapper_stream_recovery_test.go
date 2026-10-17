package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

type wrapperQuotaRepo struct {
	AccountRepository
	until  time.Time
	extra  map[string]any
	writes int
}

func (r *wrapperQuotaRepo) SetRateLimited(_ context.Context, _ int64, until time.Time) error {
	r.until = until
	r.writes++
	return nil
}
func (r *wrapperQuotaRepo) SetRateLimitedIfLater(ctx context.Context, id int64, until time.Time) error {
	if r.until.After(until) {
		return nil
	}
	return r.SetRateLimited(ctx, id, until)
}
func (r *wrapperQuotaRepo) UpdateExtra(_ context.Context, _ int64, extra map[string]any) error {
	r.extra = extra
	return nil
}

func quotaSSE(events ...string) string {
	var b strings.Builder
	for _, e := range events {
		fmt.Fprintf(&b, "event: %s\ndata: %s\n\n", gjson.Get(e, "type").String(), e)
	}
	return b.String()
}

const quotaStart = `{"type":"message_start","message":{"id":"msg-original","type":"message","role":"assistant","model":"claude-opus-5-5","content":[],"usage":{"input_tokens":10,"output_tokens":0}}}`

func quotaText(index int, value string) []string {
	encoded, _ := json.Marshal(value)
	return []string{fmt.Sprintf(`{"type":"content_block_start","index":%d,"content_block":{"type":"text","text":""}}`, index), fmt.Sprintf(`{"type":"content_block_delta","index":%d,"delta":{"type":"text_delta","text":%s}}`, index, encoded), fmt.Sprintf(`{"type":"content_block_stop","index":%d}`, index)}
}
func quotaEnd(reason string) []string {
	return []string{fmt.Sprintf(`{"type":"message_delta","delta":{"stop_reason":%q},"usage":{"output_tokens":7}}`, reason), `{"type":"message_stop","wrapper_turn_id":"receipt-new"}`}
}

func TestWrapperPartialResponsePreservesTimingAndNeverBecomesQuotaRecovery(t *testing.T) {
	t.Cleanup(func() { wrapperQuotaBlocks.Delete(201) })
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
	repo := &wrapperQuotaRepo{}
	svc := &GatewayService{rateLimitService: &RateLimitService{accountRepo: repo}}
	events := append([]string{quotaStart}, quotaText(0, "Ready to write.")...)
	events = append(events, `{"type":"error","error":{"type":"managed_partial_response","reason":"late_rate_limit","retry_safe":false,"session_reset_required":true,"usage":{"output_tokens":123}}}`)
	resp := &http.Response{Header: http.Header{}, Body: io.NopCloser(strings.NewReader(quotaSSE(events...)))}
	result, err := svc.streamWrapperWithRecovery(context.Background(), resp, c, newAnthropicAPIKeyAccountForTest(), time.Now().Add(-3*time.Second), nil, &WrapperStreamRecovery{})
	var partial *WrapperPartialResponseError
	var quota *WrapperStreamLimitError
	if !errors.As(err, &partial) || errors.As(err, &quota) {
		t.Fatalf("wrong error: %v", err)
	}
	if result.firstTokenMs == nil || *result.firstTokenMs < 3000 {
		t.Fatal("existing upstream first-token clock lost")
	}
	if result.usage.OutputTokens != 123 {
		t.Fatalf("confirmed usage lost: %+v", result.usage)
	}
	if repo.writes != 1 {
		t.Fatal("late quota must still cool the account without replaying the request")
	}
}

// An HTTP body may remain open for a long tool generation. Text must pass
// through before EOF, while the existing upstream-arrival clock stays intact.
func TestWrapperTextArrivesBeforeUpstreamToolFinishes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	reader, writer := io.Pipe()
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, quotaSSE(append([]string{quotaStart}, quotaText(0, "early real text")...)...))
		w.(http.Flusher).Flush()
		<-req.Context().Done()
	}))
	defer upstream.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", upstream.URL, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	recorder := &wrapperPipeWriter{ResponseWriter: httptest.NewRecorder(), writer: writer}
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
	done := make(chan error, 1)
	go func() {
		_, e := (&GatewayService{}).streamWrapperWithRecovery(ctx, resp, c, &Account{}, time.Now(), nil, &WrapperStreamRecovery{})
		writer.Close()
		done <- e
	}()
	text := make(chan bool, 1)
	go func() {
		defer reader.Close()
		buf := make([]byte, 1024)
		var received string
		for {
			n, e := reader.Read(buf)
			received += string(buf[:n])
			if strings.Contains(received, "early real text") {
				text <- true
				return
			}
			if e != nil {
				text <- false
				return
			}
		}
	}()
	select {
	case ok := <-text:
		if !ok {
			t.Fatal("text not forwarded")
		}
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("gateway buffered text until completion")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("disconnect did not stop forwarding")
	}
}

type wrapperPipeWriter struct {
	http.ResponseWriter
	writer *io.PipeWriter
}

func (w *wrapperPipeWriter) Write(b []byte) (int, error) { return w.writer.Write(b) }
func (w *wrapperPipeWriter) Flush()                      {}

func TestWrapperQuotaCooldown(t *testing.T) {
	t.Cleanup(func() { wrapperQuotaBlocks.Delete(201) })
	repo := &wrapperQuotaRepo{}
	s := &RateLimitService{accountRepo: repo}
	a := newAnthropicAPIKeyAccountForTest()
	future := time.Now().Add(6 * 24 * time.Hour).Truncate(time.Second)
	s.handleWrapperRateLimit(context.Background(), a, nil, []byte(fmt.Sprintf(`{"error":{"reset_at":%q}}`, fmt.Sprint(future.Unix()))))
	if !repo.until.Equal(future) {
		t.Fatalf("reset: %v", repo.until)
	}
	a.Extra["wrapper_protocol"] = "anthropic-messages-cli-v1"
	if a.IsSchedulable() || !a.IsRateLimited() {
		t.Fatal("stale snapshot remained schedulable")
	}
	// A shorter concurrent response must never reopen the account early.
	s.handleWrapperRateLimit(context.Background(), a, nil, []byte(fmt.Sprintf(`{"error":{"reset_at":%q}}`, fmt.Sprint(time.Now().Add(time.Hour).Unix()))))
	if !repo.until.Equal(future) {
		t.Fatal("reset shortened")
	}
	repo.until = time.Time{}
	for n := 0; n < 8; n++ {
		a.Extra = repo.extra
		before := time.Now()
		s.handleWrapperRateLimit(context.Background(), a, nil, []byte(`{"error":{"reset_at":"invalid"}}`))
		expected := time.Duration(30*(1<<min(n, 5))) * time.Second
		if d := repo.until.Sub(before); d < expected || d > expected+time.Second {
			t.Fatalf("backoff %d: %v", n, d)
		}
	}
}

func TestWrapperQuotaStreamRecoveryAndNextToolResult(t *testing.T) {
	t.Cleanup(func() { wrapperQuotaBlocks.Delete(201) })
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	store := newWrapperManagedTestStore()
	key := "quota-test"
	body := []byte(`{"model":"claude-opus-5-5","stream":true,"max_tokens":2000,"messages":[{"role":"user","content":"perform work"}],"tools":[{"name":"Read","description":"read","input_schema":{"type":"object","properties":{"path":{"type":"string"}}}}]}`)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
	EnableWrapperStreamRecovery(c, body)
	defer FinishWrapperStreamRecovery(c)
	r := wrapperRecovery(c)
	repo := &wrapperQuotaRepo{}
	svc := &GatewayService{rateLimitService: &RateLimitService{accountRepo: repo}}
	account := newAnthropicAPIKeyAccountForTest()
	run := func(id int64, req []byte, events []string) (*WrapperManagedAutoTurn, error) {
		turn, err := beginWrapperManagedAutoTurn(ctx, store, key)
		if err != nil {
			t.Fatal(err)
		}
		_, active, err := turn.Prepare(ctx, id, req, c)
		if err != nil || !active {
			t.Fatalf("prepare: %v active=%v", err, active)
		}
		response := &http.Response{Header: http.Header{}, Body: io.NopCloser(strings.NewReader(quotaSSE(events...)))}
		defer response.Body.Close()
		_, err = svc.streamWrapperWithRecovery(ctx, response, c, account, time.Now(), turn, r)
		return turn, err
	}
	firstText := "This is the first completed sentence. "
	events := append([]string{quotaStart}, quotaText(0, firstText)...)
	events = append(events, `{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"tool-discard","name":"Read","input":{}}}`, `{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"path\":"}}`, `{"type":"error","error":{"type":"rate_limit_error","message":"quota","reset_at":"invalid"}}`)
	first, err := run(1, body, events)
	var quotaErr *WrapperStreamLimitError
	if !errors.As(err, &quotaErr) {
		t.Fatalf("quota error: %v", err)
	}
	first.Quarantine()
	first.Release()
	if strings.Contains(recorder.Body.String(), "tool-discard") || strings.Contains(recorder.Body.String(), "rate_limit_error") {
		t.Fatal("incomplete tool or recoverable error leaked")
	}
	if repo.writes != 1 {
		t.Fatal("account not cooled")
	}
	next, err := AdvanceWrapperStreamRecovery(c)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(next), firstText) {
		t.Fatal("visible prefix missing from replacement history")
	}
	secondText := "This is the first completed sentence. Next sentence."
	events = append([]string{quotaStart}, quotaText(0, secondText)...)
	events = append(events, `{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"tool-next","name":"Read","input":{}}}`, `{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"path\":\"probe.txt\"}"}}`, `{"type":"content_block_stop","index":1}`)
	events = append(events, quotaEnd("tool_use")...)
	second, err := run(2, next, events)
	if err != nil {
		t.Fatal(err)
	}
	if !second.IsReady() {
		t.Fatal("replacement history not committed")
	}
	second.Release()
	output := recorder.Body.String()
	if strings.Count(output, "event: message_start") != 1 || strings.Count(output, "event: message_stop") != 1 || strings.Count(output, firstText) != 1 {
		t.Fatalf("invalid stitched stream: %s", output)
	}
	if r.text != firstText+"Next sentence." {
		t.Fatalf("stitched text=%q", r.text)
	}
	if gjson.Get(string(r.blocks[len(r.blocks)-1]), "input.path").String() != "probe.txt" {
		t.Fatal("tool JSON missing")
	}
	if !strings.Contains(output, `"input_tokens":20`) {
		t.Fatal("attempt usage not aggregated")
	}
	// The next request contains only what the real client received, no internal continuation prompt.
	var messages []json.RawMessage
	json.Unmarshal([]byte(gjson.GetBytes(body, "messages").Raw), &messages)
	assistant, _ := json.Marshal(map[string]any{"role": "assistant", "content": r.blocks})
	messages = append(messages, assistant, json.RawMessage(`{"role":"user","content":[{"type":"tool_result","tool_use_id":"tool-next","content":"done"}]}`))
	nextBody, _ := sjson.SetBytes(body, "messages", messages)
	third, err := beginWrapperManagedAutoTurn(ctx, store, key)
	if err != nil {
		t.Fatal(err)
	}
	defer third.Release()
	_, active, err := third.Prepare(ctx, 2, nextBody, c)
	if err != nil || !active || !third.wasReady || c.GetHeader("X-Wrapper-Ack-Turn-Id") != "receipt-new" {
		t.Fatalf("next tool result lost native receipt: active=%v ready=%v err=%v decision=%s", active, third.wasReady, err, third.skipReason)
	}
}

func TestWrapperQuotaRecoveryHistoryAndBudget(t *testing.T) {
	c := managedTestContext()
	body := []byte(`{"messages":[{"role":"user","content":"read"},{"role":"assistant","content":[{"type":"tool_use","id":"done","name":"Read","input":{}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"done","content":"completed"}]}]}`)
	EnableWrapperStreamRecovery(c, body)
	defer FinishWrapperStreamRecovery(c)
	for i := 0; i < 3; i++ {
		next, err := AdvanceWrapperStreamRecovery(c)
		if err != nil {
			t.Fatal(err)
		}
		var messages []json.RawMessage
		json.Unmarshal([]byte(gjson.GetBytes(next, "messages").Raw), &messages)
		if !validManagedBootstrapHistory(messages) {
			t.Fatal("completed tool history cannot bootstrap")
		}
	}
	if _, err := AdvanceWrapperStreamRecovery(c); err == nil {
		t.Fatal("switch budget bypassed")
	}
	if _, ok := c.Request.Context().Deadline(); !ok {
		t.Fatal("upstream selection lacks recovery deadline")
	}
}

func TestWrapperContinuationPrefix(t *testing.T) {
	for _, tc := range [][3]string{{"hello", "hello again", "hello again"}, {"already emitted sufficiently long text", "sufficiently long text more", " more"}, {"甲乙丙丁戊己庚辛壬癸", "甲乙丙丁戊己庚辛壬癸后续", "后续"}} {
		if got := wrapperContinuationPrefix(tc[0], tc[1]); got != tc[2] {
			t.Fatalf("overlap: %q", got)
		}
	}
}
