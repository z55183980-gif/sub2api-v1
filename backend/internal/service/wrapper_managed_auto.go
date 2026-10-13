package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

const (
	AutoWrapperManagedEnv       = "SUB2API_AUTO_WRAPPER_MANAGED"
	AutoWrapperManagedKeyIDsEnv = "SUB2API_AUTO_WRAPPER_MANAGED_KEY_IDS"
	wrapperManagedTTL           = time.Hour
	wrapperManagedLockTTL       = 45 * time.Second
	wrapperManagedMaxStateBytes = 2 << 20
)

type wrapperManagedState struct {
	AccountID       int64             `json:"account_id"`
	NativeSessionID string            `json:"native_session_id,omitempty"`
	Model           string            `json:"model"`
	System          json.RawMessage   `json:"system"`
	Messages        []json.RawMessage `json:"messages"`
	Receipt         string            `json:"receipt"`
	Phase           string            `json:"phase"`
	PendingToolIDs  []string          `json:"pending_tool_ids,omitempty"`
}

type WrapperManagedAutoTurn struct {
	key                string
	token              string
	state              wrapperManagedState
	store              WrapperManagedStore
	mu                 sync.Mutex
	failed             bool
	finished           bool
	stop               chan struct{}
	stopped            chan struct{}
	blocks             map[int]json.RawMessage
	toolJSON           map[int]string
	receipt            string
	terminal           bool
	stopReason         string
	requestMessages    []json.RawMessage
	wasReady           bool
	skipReason         string
	inputMessages      int
	normalizedMessages int
	matchedMessages    int
	mergedAssistants   int
	recentShape        string
}

func (t *WrapperManagedAutoTurn) Decision() (string, int, int) {
	return t.skipReason, t.inputMessages, t.normalizedMessages
}

// HistoryDecision reports only structural counts. No message content is logged.
func (t *WrapperManagedAutoTurn) HistoryDecision() (int, int, string) {
	return t.matchedMessages, t.mergedAssistants, t.recentShape
}

func managedRecentMessageShape(messages []json.RawMessage) string {
	if len(messages) > 6 {
		messages = messages[len(messages)-6:]
	}
	shape := make([]string, 0, len(messages))
	for _, message := range messages {
		role := gjson.GetBytes(message, "role").String()
		if role != "user" && role != "assistant" {
			role = "other"
		}
		blocks := managedHistoryBlocks(message)
		if blocks == nil {
			shape = append(shape, role+":text")
			continue
		}
		types := make([]string, 0, len(blocks))
		for _, item := range blocks {
			block, ok := item.(map[string]any)
			if !ok {
				types = append(types, "other")
				continue
			}
			typ, _ := block["type"].(string)
			switch typ {
			case "text", "tool_use", "tool_result", "thinking", "redacted_thinking":
				types = append(types, typ)
			default:
				types = append(types, "other")
			}
		}
		shape = append(shape, role+":"+strings.Join(types, "+"))
	}
	return strings.Join(shape, ",")
}

// A new native CLI session may be seeded with a complete, proven transcript.
// Historical tool calls must already have matching results; a fresh tool result
// needs a live handoff and cannot be imported into a new CLI process.
func validManagedBootstrapHistory(messages []json.RawMessage) bool {
	if len(messages) == 0 || gjson.GetBytes(messages[len(messages)-1], "role").String() != "user" {
		return false
	}
	pending := map[string]bool{}
	seen := map[string]bool{}
	for index, message := range messages {
		role := gjson.GetBytes(message, "role").String()
		if role != "user" && role != "assistant" {
			return false
		}
		if role == "assistant" && len(managedCanonicalAssistant(message)) == 0 {
			return false
		}
		for _, item := range managedHistoryBlocks(message) {
			block, ok := item.(map[string]any)
			if !ok {
				return false
			}
			typ, _ := block["type"].(string)
			switch typ {
			case "tool_use":
				if role != "assistant" {
					return false
				}
				id, _ := block["id"].(string)
				if id == "" || seen[id] {
					return false
				}
				seen[id], pending[id] = true, true
			case "tool_result":
				id, _ := block["tool_use_id"].(string)
				if role != "user" || index == len(messages)-1 || !pending[id] {
					return false
				}
				delete(pending, id)
			}
		}
	}
	return len(pending) == 0
}

func AutoWrapperManagedEnabledForKey(keyID int64) bool {
	if os.Getenv(AutoWrapperManagedEnv) != "1" {
		return false
	}
	for _, item := range strings.Split(os.Getenv(AutoWrapperManagedKeyIDsEnv), ",") {
		if strings.TrimSpace(item) == fmt.Sprint(keyID) {
			return true
		}
	}
	return false
}

func WrapperManagedAutoKey(keyID int64, downstreamSessionID string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("v1:%d:%s", keyID, downstreamSessionID)))
	return hex.EncodeToString(sum[:])
}

func WrapperManagedAutoSessionID(key string) string { return "sub2api-" + key }

func newWrapperManagedAutoSessionID(key string) (string, error) {
	var suffix [16]byte
	if _, err := rand.Read(suffix[:]); err != nil {
		return "", err
	}
	return WrapperManagedAutoSessionID(key) + "-" + hex.EncodeToString(suffix[:]), nil
}

func (s *GatewayService) WrapperManagedStore() WrapperManagedStore {
	store, _ := s.cache.(WrapperManagedStore)
	return store
}

func (s *GatewayService) BeginWrapperManagedAutoTurn(ctx context.Context, key string) (*WrapperManagedAutoTurn, error) {
	return beginWrapperManagedAutoTurn(ctx, s.WrapperManagedStore(), key)
}

func (s *GatewayService) WrapperManagedAutoState(ctx context.Context, key string) (bool, int64, error) {
	store := s.WrapperManagedStore()
	if store == nil {
		return false, 0, errors.New("managed state store unavailable")
	}
	data, err := store.GetWrapperManagedState(ctx, key)
	if err != nil {
		return false, 0, err
	}
	if len(data) == 0 {
		return false, 0, nil
	}
	var state wrapperManagedState
	if err := json.Unmarshal(data, &state); err != nil {
		return false, 0, err
	}
	if state.Phase == "" && state.AccountID == 0 {
		return false, 0, nil
	}
	if state.AccountID <= 0 || (state.Phase != "ready" && state.Phase != "quarantined" && state.Phase != "pending") {
		return true, state.AccountID, errors.New("managed session state is invalid")
	}
	return true, state.AccountID, nil
}

func beginWrapperManagedAutoTurn(ctx context.Context, store WrapperManagedStore, key string) (*WrapperManagedAutoTurn, error) {
	if store == nil {
		return nil, errors.New("managed state store unavailable")
	}
	token, acquired, err := store.AcquireWrapperManagedLock(ctx, key, wrapperManagedLockTTL)
	if err != nil {
		return nil, err
	}
	if !acquired {
		return nil, errors.New("managed session already has an active turn")
	}
	turn := &WrapperManagedAutoTurn{key: key, token: token, store: store, stop: make(chan struct{}), stopped: make(chan struct{}), blocks: map[int]json.RawMessage{}, toolJSON: map[int]string{}}
	go turn.keepLock()
	data, err := store.GetWrapperManagedState(ctx, key)
	if err != nil {
		turn.Release()
		return nil, err
	}
	if len(data) > 0 {
		if err := json.Unmarshal(data, &turn.state); err != nil {
			turn.Release()
			return nil, fmt.Errorf("managed state corrupted: %w", err)
		}
	}
	return turn, nil
}

func (t *WrapperManagedAutoTurn) keepLock() {
	defer close(t.stopped)
	ticker := time.NewTicker(wrapperManagedLockTTL / 3)
	defer ticker.Stop()
	for {
		select {
		case <-t.stop:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			ok, err := t.store.RefreshWrapperManagedLock(ctx, t.key, t.token, wrapperManagedLockTTL)
			cancel()
			if err != nil || !ok {
				t.mu.Lock()
				t.failed = true
				t.mu.Unlock()
				return
			}
		}
	}
}

func (t *WrapperManagedAutoTurn) Release() {
	t.mu.Lock()
	if t.finished {
		t.mu.Unlock()
		return
	}
	t.finished = true
	close(t.stop)
	t.mu.Unlock()
	<-t.stopped
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = t.store.ReleaseWrapperManagedLock(ctx, t.key, t.token)
}

func (t *WrapperManagedAutoTurn) abandonNativeSession(ctx context.Context, reason string) error {
	t.skipReason = reason
	t.state = wrapperManagedState{}
	t.wasReady = false
	return t.save(ctx)
}

func (t *WrapperManagedAutoTurn) save(ctx context.Context) error {
	data, err := json.Marshal(t.state)
	if err != nil {
		return err
	}
	if len(data) > wrapperManagedMaxStateBytes {
		return errors.New("managed session history exceeds 2 MiB")
	}
	return t.store.SetWrapperManagedState(ctx, t.key, data, wrapperManagedTTL)
}

func (t *WrapperManagedAutoTurn) Quarantine() {
	t.state.Phase = "quarantined"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if t.save(ctx) != nil {
		// Keep the quarantine marker even when an oversized transcript cannot
		// be saved; accepting the previous receipt would risk duplicate turns.
		t.state.Messages = nil
		t.state.Receipt = ""
		t.state.PendingToolIDs = nil
		_ = t.save(ctx)
	}
}

func (t *WrapperManagedAutoTurn) IsReady() bool { return t.state.Phase == "ready" }

func normalizedWrapperManagedBody(body []byte, betaHeader string) ([]byte, error) {
	body = StripEmptyTextBlocks(body)
	body = FilterWebSearchHistoryBlocks(body, gjson.GetBytes(body, "model").String())
	body = stripDeferredToolCacheControl(body)
	var err error
	body, err = normalizeWrapperMessages(body)
	if err != nil {
		return nil, err
	}
	if sanitized, changed := sanitizeAnthropicBodyForBetaTokens(body, betaHeader); changed {
		body = sanitized
	}
	return body, nil
}

func managedHistoryMessage(raw json.RawMessage) json.RawMessage {
	// Wrapper stores only client-visible blocks, without thinking or cache hints.
	var message map[string]any
	if json.Unmarshal(raw, &message) != nil {
		return raw
	}
	for key := range message {
		if key != "role" && key != "content" {
			delete(message, key)
		}
	}
	if blocks, ok := message["content"].([]any); ok {
		filtered := make([]any, 0, len(blocks))
		for _, item := range blocks {
			block, ok := item.(map[string]any)
			if !ok {
				filtered = append(filtered, item)
				continue
			}
			if block["type"] == "thinking" || block["type"] == "redacted_thinking" {
				continue
			}
			delete(block, "cache_control")
			filtered = append(filtered, block)
		}
		message["content"] = filtered
		// Claude Code may turn a cached single text block into a plain string
		// on its next request. Both forms represent the same visible message.
		if len(filtered) == 1 {
			if block, ok := filtered[0].(map[string]any); ok && block["type"] == "text" {
				if value, ok := block["text"].(string); ok {
					message["content"] = value
				}
			}
		}
	}
	encoded, _ := json.Marshal(message)
	return encoded
}

func managedHistoryBlocks(raw json.RawMessage) []any {
	var message map[string]any
	if json.Unmarshal(raw, &message) != nil {
		return nil
	}
	blocks, _ := message["content"].([]any)
	return blocks
}

func hasManagedToolResult(raw json.RawMessage) bool {
	for _, item := range managedHistoryBlocks(raw) {
		block, ok := item.(map[string]any)
		if ok && block["type"] == "tool_result" {
			return true
		}
	}
	return false
}

func managedCanonicalAssistant(raw json.RawMessage) json.RawMessage {
	var message map[string]any
	if json.Unmarshal(raw, &message) != nil {
		return nil
	}
	blocks := managedHistoryBlocks(raw)
	if blocks == nil {
		content, ok := message["content"].(string)
		if !ok {
			return nil
		}
		blocks = []any{map[string]any{"type": "text", "text": content}}
	}
	filtered := make([]any, 0, len(blocks))
	for _, item := range blocks {
		block, ok := item.(map[string]any)
		if !ok {
			return nil
		}
		switch block["type"] {
		case "thinking", "redacted_thinking":
			continue
		case "text":
			filtered = append(filtered, map[string]any{"type": "text", "text": block["text"]})
		case "tool_use":
			filtered = append(filtered, map[string]any{"type": "tool_use", "id": block["id"], "name": block["name"], "input": block["input"]})
		default:
			return nil
		}
	}
	encoded, _ := json.Marshal(map[string]any{"role": "assistant", "content": filtered})
	return encoded
}

func equalManagedSystem(a, b json.RawMessage) bool {
	if len(a) == 0 || string(a) == "null" {
		return len(b) == 0 || string(b) == "null"
	}
	if len(b) == 0 || string(b) == "null" {
		return false
	}
	var left, right any
	if json.Unmarshal(a, &left) != nil || json.Unmarshal(b, &right) != nil {
		return false
	}
	return reflect.DeepEqual(left, right)
}

func equalManagedMessage(a, b json.RawMessage) bool {
	if gjson.GetBytes(b, "role").String() == "assistant" {
		left := managedCanonicalAssistant(a)
		right := managedCanonicalAssistant(b)
		return len(left) > 0 && len(right) > 0 && string(left) == string(right)
	}
	return string(managedHistoryMessage(a)) == string(managedHistoryMessage(b))
}

// matchManagedHistoryPrefix proves that the complete client history (except its
// final, fresh user message) is the history already executed by the CLI.
// Some clients split one assistant turn into adjacent text and tool_use
// messages. Joining only consecutive assistant blocks preserves their order and
// never treats an unexecuted user message or tool result as saved history.
func matchManagedHistoryPrefix(request, saved []json.RawMessage) (int, int, string) {
	if len(request) == 0 {
		return 0, 0, "history_length_changed"
	}
	client := 0
	merged := 0
	for _, previous := range saved {
		if client >= len(request)-1 {
			return client, merged, "history_length_changed"
		}
		if equalManagedMessage(request[client], previous) {
			client++
			continue
		}
		if gjson.GetBytes(previous, "role").String() != "assistant" || gjson.GetBytes(request[client], "role").String() != "assistant" {
			return client, merged, "history_changed"
		}
		var expected struct {
			Content []json.RawMessage `json:"content"`
		}
		if err := json.Unmarshal(managedCanonicalAssistant(previous), &expected); err != nil || len(expected.Content) == 0 {
			return client, merged, "history_changed"
		}
		combined := make([]json.RawMessage, 0, len(expected.Content))
		start := client
		for client < len(request)-1 && gjson.GetBytes(request[client], "role").String() == "assistant" {
			var part struct {
				Content []json.RawMessage `json:"content"`
			}
			if err := json.Unmarshal(managedCanonicalAssistant(request[client]), &part); err != nil || len(part.Content) == 0 {
				return client, merged, "history_changed"
			}
			combined = append(combined, part.Content...)
			client++
			if len(combined) > len(expected.Content) {
				return client, merged, "history_changed"
			}
			if len(combined) == len(expected.Content) {
				if !reflect.DeepEqual(combined, expected.Content) {
					return client, merged, "history_changed"
				}
				merged += client - start - 1
				break
			}
		}
		if len(combined) != len(expected.Content) {
			return client, merged, "history_changed"
		}
	}
	if client != len(request)-1 {
		return client, merged, "extra_history_messages"
	}
	return client, merged, ""
}

// Prepare accepts a complete downstream transcript. The initial managed turn
// seeds the native session with validated history; later turns send only the
// fresh user message.
func (t *WrapperManagedAutoTurn) Prepare(ctx context.Context, accountID int64, body []byte, c *gin.Context) ([]byte, bool, error) {
	t.inputMessages = int(gjson.GetBytes(body, "messages.#").Int())
	normalized, err := normalizedWrapperManagedBody(body, c.GetHeader("anthropic-beta"))
	if err != nil {
		return nil, false, err
	}
	var request struct {
		Model    string            `json:"model"`
		System   json.RawMessage   `json:"system"`
		Messages []json.RawMessage `json:"messages"`
	}
	if err := json.Unmarshal(normalized, &request); err != nil {
		return nil, false, err
	}
	t.normalizedMessages = len(request.Messages)
	t.recentShape = managedRecentMessageShape(request.Messages)
	// Wrapper accepts context_management as an unsupported hint in both modes.
	// Claude Code sends it on ordinary turns, so it must not block native reuse.
	if gjson.GetBytes(normalized, "metadata.mcp_servers.#").Int() > 0 {
		if t.state.Phase != "" {
			if err := t.abandonNativeSession(ctx, "mcp_servers"); err != nil {
				return nil, false, err
			}
		}
		t.skipReason = "mcp_servers"
		return body, false, nil
	}
	if t.state.AccountID > 0 && t.state.AccountID != accountID {
		if err := t.abandonNativeSession(ctx, "account_changed"); err != nil {
			return nil, false, err
		}
		t.skipReason = "account_changed"
		return body, false, nil
	}
	resyncReason := ""
	if t.state.Phase != "" && t.state.Phase != "ready" {
		resyncReason = "incomplete_previous_turn"
	} else if t.state.Phase == "ready" {
		switch {
		case t.state.Model != request.Model || !equalManagedSystem(t.state.System, request.System):
			resyncReason = "model_or_system_changed"
		case t.state.Receipt == "":
			resyncReason = "receipt_unavailable"
		default:
			t.matchedMessages, t.mergedAssistants, resyncReason = matchManagedHistoryPrefix(request.Messages, t.state.Messages)
		}
	}
	if resyncReason != "" {
		// Never resume a native transcript whose client-visible history is
		// uncertain. A proven full downstream transcript can seed a fresh ID;
		// otherwise keep this request on the stateless path.
		t.skipReason = resyncReason
		if err := t.abandonNativeSession(ctx, resyncReason); err != nil {
			return nil, false, err
		}
	}
	if len(request.Messages) == 0 {
		if t.state.Phase != "" {
			if err := t.abandonNativeSession(ctx, "empty_history"); err != nil {
				return nil, false, err
			}
		}
		t.skipReason = "empty_history"
		return body, false, nil
	}
	if t.state.Phase == "" && !validManagedBootstrapHistory(request.Messages) {
		if t.skipReason == "" {
			t.skipReason = "unproven_initial_history"
		}
		return body, false, nil
	}
	fresh := request.Messages[len(request.Messages)-1]
	if gjson.GetBytes(fresh, "role").String() != "user" {
		if t.state.Phase != "" || resyncReason != "" {
			if err := t.abandonNativeSession(ctx, "no_fresh_user_turn"); err != nil {
				return nil, false, err
			}
		}
		t.skipReason = "no_fresh_user_turn"
		return body, false, nil
	}
	if t.state.Phase == "" && hasManagedToolResult(fresh) {
		t.skipReason = "pending_tool_result_without_handoff"
		return body, false, nil
	}
	if t.state.Phase == "ready" && len(t.state.PendingToolIDs) == 0 && hasManagedToolResult(fresh) {
		if err := t.abandonNativeSession(ctx, "unexpected_tool_result"); err != nil {
			return nil, false, err
		}
		t.skipReason = "unexpected_tool_result"
		return body, false, nil
	}
	if t.state.Phase == "" {
		prefix := make([]json.RawMessage, 0, len(request.Messages)-1)
		for _, message := range request.Messages[:len(request.Messages)-1] {
			prefix = append(prefix, managedHistoryMessage(message))
		}
		nativeSessionID, err := newWrapperManagedAutoSessionID(t.key)
		if err != nil {
			return nil, true, err
		}
		bootstrap := wrapperManagedState{AccountID: accountID, NativeSessionID: nativeSessionID, Model: request.Model, System: request.System, Messages: prefix, Phase: "pending"}
		encoded, err := json.Marshal(bootstrap)
		if err != nil {
			return nil, false, err
		}
		// Leave room for the first assistant turn. A new oversized history can
		// still be forwarded statelessly without creating a broken managed state.
		if len(encoded) > wrapperManagedMaxStateBytes-(128<<10) {
			t.skipReason = "bootstrap_history_too_large"
			return body, false, nil
		}
		t.state = bootstrap
	} else {
		// Keep the legacy ID for sessions created before native_session_id was
		// persisted. New sessions always carry a unique generation suffix.
		if t.state.NativeSessionID == "" {
			t.state.NativeSessionID = WrapperManagedAutoSessionID(t.key)
		}
	}
	if len(t.state.PendingToolIDs) > 0 {
		blocks := managedHistoryBlocks(fresh)
		if len(blocks) != len(t.state.PendingToolIDs) {
			if err := t.abandonNativeSession(ctx, "pending_tool_results_changed"); err != nil {
				return nil, false, err
			}
			t.skipReason = "pending_tool_results_changed"
			return body, false, nil
		}
		expected := map[string]bool{}
		for _, id := range t.state.PendingToolIDs {
			expected[id] = true
		}
		for _, item := range blocks {
			block, ok := item.(map[string]any)
			if !ok || block["type"] != "tool_result" {
				if err := t.abandonNativeSession(ctx, "pending_tool_results_changed"); err != nil {
					return nil, false, err
				}
				t.skipReason = "pending_tool_results_changed"
				return body, false, nil
			}
			id, _ := block["tool_use_id"].(string)
			if !expected[id] {
				if err := t.abandonNativeSession(ctx, "pending_tool_results_changed"); err != nil {
					return nil, false, err
				}
				t.skipReason = "pending_tool_results_changed"
				return body, false, nil
			}
			delete(expected, id)
			switch content := block["content"].(type) {
			case string:
			case []any:
				for _, part := range content {
					p, ok := part.(map[string]any)
					if !ok || p["type"] != "text" {
						if err := t.abandonNativeSession(ctx, "non_text_tool_result"); err != nil {
							return nil, false, err
						}
						t.skipReason = "non_text_tool_result"
						return body, false, nil
					}
					if _, ok := p["text"].(string); !ok {
						if err := t.abandonNativeSession(ctx, "invalid_tool_result"); err != nil {
							return nil, false, err
						}
						t.skipReason = "invalid_tool_result"
						return body, false, nil
					}
				}
			default:
				if err := t.abandonNativeSession(ctx, "invalid_tool_result"); err != nil {
					return nil, false, err
				}
				t.skipReason = "invalid_tool_result"
				return body, false, nil
			}
			if isError, exists := block["is_error"]; exists {
				if _, ok := isError.(bool); !ok {
					if err := t.abandonNativeSession(ctx, "invalid_tool_result"); err != nil {
						return nil, false, err
					}
					t.skipReason = "invalid_tool_result"
					return body, false, nil
				}
			}
		}
	}
	t.wasReady = t.state.Phase == "ready"
	t.requestMessages = append([]json.RawMessage(nil), request.Messages...)
	t.state.Phase = "pending"
	if err := t.save(ctx); err != nil {
		return nil, true, err
	}
	forwardMessages := append(append([]byte("["), fresh...), ']')
	if !t.wasReady {
		forwardMessages = []byte(gjson.GetBytes(normalized, "messages").Raw)
	}
	updated, err := sjson.SetRawBytes(normalized, "messages", forwardMessages)
	if err != nil {
		return nil, true, err
	}
	c.Request.Header.Set("X-Wrapper-Session-Mode", "managed")
	c.Request.Header.Set("X-Wrapper-Session-Id", t.state.NativeSessionID)
	c.Request.Header.Set("X-Claude-Code-Session-Id", t.state.NativeSessionID)
	if t.wasReady && t.state.Receipt != "" {
		c.Request.Header.Set("X-Wrapper-Ack-Turn-Id", t.state.Receipt)
	}
	return updated, true, nil
}

func (t *WrapperManagedAutoTurn) Observe(data []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	event := gjson.GetBytes(data, "type").String()
	index := int(gjson.GetBytes(data, "index").Int())
	switch event {
	case "content_block_start":
		block := gjson.GetBytes(data, "content_block")
		if block.Exists() {
			t.blocks[index] = json.RawMessage(block.Raw)
		}
	case "content_block_delta":
		block := t.blocks[index]
		delta := gjson.GetBytes(data, "delta")
		switch delta.Get("type").String() {
		case "text_delta":
			previous := gjson.GetBytes(block, "text").String()
			if next, err := sjson.SetBytes(block, "text", previous+delta.Get("text").String()); err == nil {
				t.blocks[index] = next
			}
		case "input_json_delta":
			t.toolJSON[index] += delta.Get("partial_json").String()
		}
	case "content_block_stop":
		if raw := t.toolJSON[index]; raw != "" {
			var obj any
			if json.Unmarshal([]byte(raw), &obj) == nil {
				if next, err := sjson.SetRawBytes(t.blocks[index], "input", []byte(raw)); err == nil {
					t.blocks[index] = next
				} else {
					t.failed = true
				}
			} else {
				t.failed = true
			}
		}
	case "message_stop":
		t.receipt = gjson.GetBytes(data, "wrapper_turn_id").String()
		t.terminal = t.receipt != ""
	case "message_delta":
		t.stopReason = gjson.GetBytes(data, "delta.stop_reason").String()
	}
}

func (t *WrapperManagedAutoTurn) Commit(ctx context.Context) error {
	t.mu.Lock()
	if t.failed || !t.terminal {
		t.mu.Unlock()
		return errors.New("managed stream did not complete safely")
	}
	blocks := make([]json.RawMessage, 0, len(t.blocks))
	for i := 0; i < len(t.blocks); i++ {
		block, ok := t.blocks[i]
		if !ok {
			t.mu.Unlock()
			return errors.New("managed stream block missing")
		}
		if gjson.GetBytes(block, "type").String() != "thinking" {
			blocks = append(blocks, block)
		}
	}
	receipt := t.receipt
	t.mu.Unlock()
	if len(blocks) == 0 {
		return errors.New("managed stream has no assistant blocks")
	}
	if t.stopReason == "tool_use" {
		ids := make([]string, 0)
		seen := make(map[string]bool)
		for _, block := range blocks {
			if gjson.GetBytes(block, "type").String() == "tool_use" {
				id := gjson.GetBytes(block, "id").String()
				if id == "" || seen[id] {
					return errors.New("tool_use stream has missing or duplicate tool IDs")
				}
				seen[id] = true
				ids = append(ids, id)
			}
		}
		if len(ids) == 0 {
			return errors.New("tool_use stream has no tool IDs")
		}
		t.state.PendingToolIDs = ids
	} else {
		if t.stopReason == "" {
			return errors.New("managed stream has no stop reason")
		}
		for _, block := range blocks {
			if gjson.GetBytes(block, "type").String() == "tool_use" {
				return errors.New("managed stream has tool_use without tool stop reason")
			}
		}
		t.state.PendingToolIDs = nil
	}
	assistant, err := json.Marshal(struct {
		Role    string            `json:"role"`
		Content []json.RawMessage `json:"content"`
	}{"assistant", blocks})
	if err != nil {
		return err
	}
	if len(t.requestMessages) == 0 {
		return errors.New("managed request has no messages")
	}
	if t.stopReason != "tool_use" && t.stopReason != "end_turn" && t.stopReason != "max_tokens" && t.stopReason != "stop_sequence" {
		return errors.New("managed stream has unsupported stop reason")
	}
	previous := append([]json.RawMessage(nil), t.state.Messages...)
	t.state.Messages = append(previous, managedHistoryMessage(t.requestMessages[len(t.requestMessages)-1]), assistant)
	t.state.Receipt = receipt
	t.state.Phase = "ready"
	return t.save(ctx)
}
