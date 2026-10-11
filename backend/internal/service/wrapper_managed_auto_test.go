package service

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

type wrapperManagedTestStore struct {
	mu     sync.Mutex
	states map[string][]byte
	locks  map[string]string
}

func newWrapperManagedTestStore() *wrapperManagedTestStore {
	return &wrapperManagedTestStore{states: map[string][]byte{}, locks: map[string]string{}}
}
func (s *wrapperManagedTestStore) GetWrapperManagedState(_ context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]byte(nil), s.states[key]...), nil
}
func (s *wrapperManagedTestStore) SetWrapperManagedState(_ context.Context, key string, value []byte, _ time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states[key] = append([]byte(nil), value...)
	return nil
}
func (s *wrapperManagedTestStore) AcquireWrapperManagedLock(_ context.Context, key string, _ time.Duration) (string, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.locks[key] != "" {
		return "", false, nil
	}
	s.locks[key] = "token"
	return "token", true, nil
}
func (s *wrapperManagedTestStore) RefreshWrapperManagedLock(_ context.Context, key, token string, _ time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.locks[key] == token, nil
}
func (s *wrapperManagedTestStore) ReleaseWrapperManagedLock(_ context.Context, key, token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.locks[key] == token {
		delete(s.locks, key)
	}
	return nil
}

func managedTestContext() *gin.Context {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	return c
}

func TestWrapperManagedAutoFullHistoryToolHandoff(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	store := newWrapperManagedTestStore()
	key := WrapperManagedAutoKey(42, "conversation-a")
	first, err := beginWrapperManagedAutoTurn(ctx, store, key)
	if err != nil {
		t.Fatal(err)
	}
	firstBody := []byte(`{"model":"sonnet","system":"fixed","stream":true,"messages":[{"role":"user","content":"calculate"}]}`)
	c := managedTestContext()
	wire, active, err := first.Prepare(ctx, 1, firstBody, c)
	if err != nil || !active {
		t.Fatalf("first prepare: %v %v", active, err)
	}
	if gjson.GetBytes(wire, "messages.#").Int() != 1 || c.GetHeader("X-Wrapper-Session-Mode") != "managed" {
		t.Fatal("initial managed request not prepared")
	}
	first.Observe([]byte(`{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_1","name":"lookup","input":{}}}`))
	first.Observe([]byte(`{"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{\"key\":\"a\"}"}}`))
	first.Observe([]byte(`{"type":"content_block_stop","index":0}`))
	first.Observe([]byte(`{"type":"message_delta","delta":{"stop_reason":"tool_use"}}`))
	first.Observe([]byte(`{"type":"message_stop","wrapper_turn_id":"11111111-1111-4111-8111-111111111111"}`))
	if err := first.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	first.Release()
	stateRaw, _ := store.GetWrapperManagedState(ctx, key)
	var state wrapperManagedState
	if err := json.Unmarshal(stateRaw, &state); err != nil {
		t.Fatal(err)
	}
	if len(state.Messages) != 2 || gjson.GetBytes(state.Messages[1], "content.0.input.key").String() != "a" {
		t.Fatalf("tool assistant not saved: %s", stateRaw)
	}
	second, err := beginWrapperManagedAutoTurn(ctx, store, key)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()
	secondBody := `{"model":"sonnet","system":"fixed","stream":true,"messages":[{"role":"user","content":"calculate"},` + string(state.Messages[1]) + `,{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_1","content":"42"}]}]}`
	c = managedTestContext()
	wire, active, err = second.Prepare(ctx, 1, []byte(secondBody), c)
	if err != nil || !active {
		t.Fatalf("second prepare: %v %v", active, err)
	}
	if gjson.GetBytes(wire, "messages.#").Int() != 1 || !strings.Contains(string(wire), `"tool_result"`) {
		t.Fatal("tool result not incremental")
	}
	if c.GetHeader("X-Wrapper-Ack-Turn-Id") != "11111111-1111-4111-8111-111111111111" {
		t.Fatal("missing receipt")
	}
}

func TestWrapperManagedAutoRejectsUnprovenHistory(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	store := newWrapperManagedTestStore()
	key := WrapperManagedAutoKey(42, "conversation-b")
	turn, _ := beginWrapperManagedAutoTurn(ctx, store, key)
	defer turn.Release()
	body := []byte(`{"model":"sonnet","messages":[{"role":"user","content":"one"},{"role":"assistant","content":[{"type":"tool_use","id":"toolu_1","name":"lookup","input":{}}]},{"role":"user","content":"three"}]}`)
	_, active, err := turn.Prepare(ctx, 1, body, managedTestContext())
	if err != nil || active {
		t.Fatalf("unresolved historical tool call must stay stateless: %v %v", active, err)
	}
}

func TestWrapperManagedAutoSeedsHistoryThenSendsOnlyFreshTurn(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	store := newWrapperManagedTestStore()
	key := WrapperManagedAutoKey(42, "conversation-seeded")
	first, err := beginWrapperManagedAutoTurn(ctx, store, key)
	if err != nil {
		t.Fatal(err)
	}
	firstBody := []byte(`{"model":"sonnet","stream":true,"messages":[{"role":"user","content":"old question"},{"role":"assistant","content":"old answer"},{"role":"user","content":"new question"}]}`)
	wire, active, err := first.Prepare(ctx, 1, firstBody, managedTestContext())
	if err != nil || !active || gjson.GetBytes(wire, "messages.#").Int() != 3 {
		t.Fatalf("initial full history was not forwarded: active=%v err=%v body=%s", active, err, wire)
	}
	first.Observe([]byte(`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":"answer"}}`))
	first.Observe([]byte(`{"type":"content_block_stop","index":0}`))
	first.Observe([]byte(`{"type":"message_delta","delta":{"stop_reason":"end_turn"}}`))
	first.Observe([]byte(`{"type":"message_stop","wrapper_turn_id":"11111111-1111-4111-8111-111111111111"}`))
	if err := first.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	first.Release()
	stateRaw, _ := store.GetWrapperManagedState(ctx, key)
	var state wrapperManagedState
	if err := json.Unmarshal(stateRaw, &state); err != nil || len(state.Messages) != 4 {
		t.Fatalf("seeded history not committed: %v %s", err, stateRaw)
	}
	second, err := beginWrapperManagedAutoTurn(ctx, store, key)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Release()
	secondBody, _ := json.Marshal(map[string]any{"model": "sonnet", "stream": true, "messages": append(state.Messages, json.RawMessage(`{"role":"user","content":"follow up"}`))})
	wire, active, err = second.Prepare(ctx, 1, secondBody, managedTestContext())
	if err != nil || !active || gjson.GetBytes(wire, "messages.#").Int() != 1 || gjson.GetBytes(wire, "messages.0.content").String() != "follow up" {
		t.Fatalf("subsequent turn was not incremental: active=%v err=%v body=%s", active, err, wire)
	}
}

func TestWrapperManagedAutoFiveTurnConversation(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	store := newWrapperManagedTestStore()
	key := WrapperManagedAutoKey(42, "conversation-five")
	var history []json.RawMessage
	previousReceipt := ""
	for i := 0; i < 5; i++ {
		user, _ := json.Marshal(map[string]any{"role": "user", "content": "question"})
		history = append(history, user)
		body, _ := json.Marshal(map[string]any{"model": "sonnet", "stream": true, "messages": history})
		turn, err := beginWrapperManagedAutoTurn(ctx, store, key)
		if err != nil {
			t.Fatal(err)
		}
		c := managedTestContext()
		wire, active, err := turn.Prepare(ctx, 1, body, c)
		if err != nil || !active || gjson.GetBytes(wire, "messages.#").Int() != 1 {
			t.Fatalf("turn %d did not send one new message: active=%v err=%v", i+1, active, err)
		}
		if c.GetHeader("X-Wrapper-Ack-Turn-Id") != previousReceipt {
			t.Fatalf("turn %d did not acknowledge previous turn", i+1)
		}
		turn.Observe([]byte(`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`))
		turn.Observe([]byte(`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"answer"}}`))
		turn.Observe([]byte(`{"type":"content_block_stop","index":0}`))
		turn.Observe([]byte(`{"type":"message_delta","delta":{"stop_reason":"end_turn"}}`))
		previousReceipt = fmt.Sprintf("00000000-0000-4000-8000-%012d", i+1)
		turn.Observe([]byte(fmt.Sprintf(`{"type":"message_stop","wrapper_turn_id":%q}`, previousReceipt)))
		if err := turn.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		turn.Release()
		stateRaw, _ := store.GetWrapperManagedState(ctx, key)
		var state wrapperManagedState
		if err := json.Unmarshal(stateRaw, &state); err != nil || state.Phase != "ready" || len(state.Messages) != 2*(i+1) {
			t.Fatalf("turn %d state invalid: %v", i+1, err)
		}
		history = append(history, state.Messages[len(state.Messages)-1])
	}
}

func TestWrapperManagedAutoRejectsChangedHistoryAndIncompleteStream(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	store := newWrapperManagedTestStore()
	key := WrapperManagedAutoKey(42, "conversation-failure")
	first, _ := beginWrapperManagedAutoTurn(ctx, store, key)
	_, active, err := first.Prepare(ctx, 1, []byte(`{"model":"sonnet","stream":true,"messages":[{"role":"user","content":"question"}]}`), managedTestContext())
	if err != nil || !active {
		t.Fatal(err)
	}
	if err := first.Commit(ctx); err == nil {
		t.Fatal("incomplete stream must not be committed")
	}
	first.Quarantine()
	first.Release()
	second, _ := beginWrapperManagedAutoTurn(ctx, store, key)
	defer second.Release()
	_, active, err = second.Prepare(ctx, 1, []byte(`{"model":"sonnet","stream":true,"messages":[{"role":"user","content":"question"}]}`), managedTestContext())
	if err == nil || !active {
		t.Fatal("quarantined turn must not resume")
	}
}

func TestWrapperManagedAutoRejectsChangedHistory(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	store := newWrapperManagedTestStore()
	key := WrapperManagedAutoKey(42, "conversation-changed")
	first, _ := beginWrapperManagedAutoTurn(ctx, store, key)
	_, active, err := first.Prepare(ctx, 1, []byte(`{"model":"sonnet","stream":true,"messages":[{"role":"user","content":"original"}]}`), managedTestContext())
	if err != nil || !active {
		t.Fatal(err)
	}
	first.Observe([]byte(`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":"answer"}}`))
	first.Observe([]byte(`{"type":"content_block_stop","index":0}`))
	first.Observe([]byte(`{"type":"message_delta","delta":{"stop_reason":"end_turn"}}`))
	first.Observe([]byte(`{"type":"message_stop","wrapper_turn_id":"11111111-1111-4111-8111-111111111111"}`))
	if err := first.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	first.Release()
	second, _ := beginWrapperManagedAutoTurn(ctx, store, key)
	defer second.Release()
	body := []byte(`{"model":"sonnet","stream":true,"messages":[{"role":"user","content":"changed"},{"role":"assistant","content":[{"type":"text","text":"answer"}]},{"role":"user","content":"next"}]}`)
	_, active, err = second.Prepare(ctx, 1, body, managedTestContext())
	if err == nil || !active || !strings.Contains(err.Error(), "history differs") {
		t.Fatalf("changed history must fail closed: active=%v err=%v", active, err)
	}
}

func TestWrapperManagedAutoSSEPassthroughIntegration(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	store := newWrapperManagedTestStore()
	key := WrapperManagedAutoKey(42, "conversation-stream")
	turn, err := beginWrapperManagedAutoTurn(ctx, store, key)
	if err != nil {
		t.Fatal(err)
	}
	defer turn.Release()
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	_, active, err := turn.Prepare(ctx, 1, []byte(`{"model":"sonnet","stream":true,"messages":[{"role":"user","content":"hello"}]}`), c)
	if err != nil || !active {
		t.Fatal(err)
	}
	const receipt = "11111111-1111-4111-8111-111111111111"
	sse := "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":2,\"cache_read_input_tokens\":123,\"cache_creation_input_tokens\":45}}}\n\n" +
		"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n" +
		"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"world\"}}\n\n" +
		"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n" +
		"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":3}}\n\n" +
		"event: message_stop\ndata: {\"type\":\"message_stop\",\"wrapper_turn_id\":\"" + receipt + "\"}\n\n"
	response := &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"text/event-stream"}}, Body: io.NopCloser(strings.NewReader(sse))}
	svc := newStreamingResponseTestGatewayService()
	result, err := svc.handleStreamingResponseAnthropicAPIKeyPassthrough(ctx, response, c, &Account{ID: 1}, time.Now(), "sonnet", turn)
	if err != nil || result.usage.CacheReadInputTokens != 123 || result.usage.CacheCreationInputTokens != 45 {
		t.Fatalf("passthrough usage: result=%+v err=%v", result, err)
	}
	if !strings.Contains(recorder.Body.String(), `"wrapper_turn_id":"`+receipt+`"`) {
		t.Fatal("receipt was lost in SSE forwarding")
	}
	if err := turn.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	stateRaw, _ := store.GetWrapperManagedState(ctx, key)
	if gjson.GetBytes(stateRaw, "messages.1.content.0.text").String() != "world" || gjson.GetBytes(stateRaw, "receipt").String() != receipt {
		t.Fatal("forwarded assistant content or receipt was not saved")
	}
}

func TestWrapperManagedAutoRejectsUnmatchedToolResult(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx := context.Background()
	store := newWrapperManagedTestStore()
	key := WrapperManagedAutoKey(42, "conversation-tool-id")
	first, _ := beginWrapperManagedAutoTurn(ctx, store, key)
	_, active, err := first.Prepare(ctx, 1, []byte(`{"model":"sonnet","stream":true,"messages":[{"role":"user","content":"lookup"}]}`), managedTestContext())
	if err != nil || !active {
		t.Fatal(err)
	}
	first.Observe([]byte(`{"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"toolu_expected","name":"lookup","input":{}}}`))
	first.Observe([]byte(`{"type":"content_block_stop","index":0}`))
	first.Observe([]byte(`{"type":"message_delta","delta":{"stop_reason":"tool_use"}}`))
	first.Observe([]byte(`{"type":"message_stop","wrapper_turn_id":"11111111-1111-4111-8111-111111111111"}`))
	if err := first.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	first.Release()
	second, _ := beginWrapperManagedAutoTurn(ctx, store, key)
	defer second.Release()
	body := []byte(`{"model":"sonnet","stream":true,"messages":[{"role":"user","content":"lookup"},{"role":"assistant","content":[{"type":"tool_use","id":"toolu_expected","name":"lookup","input":{}}]},{"role":"user","content":[{"type":"tool_result","tool_use_id":"toolu_wrong","content":"42"}]}]}`)
	_, active, err = second.Prepare(ctx, 1, body, managedTestContext())
	if err == nil || !active || !strings.Contains(err.Error(), "tool result ID") {
		t.Fatalf("unmatched result must fail closed: active=%v err=%v", active, err)
	}
}
