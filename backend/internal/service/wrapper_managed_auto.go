package service

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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

// Invalid image payloads are rejected before changing a pending handoff. Clients
// can correct the payload using the same receipt; do not silently fall back.
type WrapperManagedPayloadError struct {
	Status int
	Reason string
}

func (e *WrapperManagedPayloadError) Error() string { return e.Reason }

type wrapperManagedState struct {
	AccountID       int64                 `json:"account_id"`
	NativeSessionID string                `json:"native_session_id,omitempty"`
	Model           string                `json:"model"`
	System          json.RawMessage       `json:"system"`
	Messages        []json.RawMessage     `json:"messages"`
	Proofs          []managedMessageProof `json:"proofs,omitempty"`
	Recent          []json.RawMessage     `json:"recent,omitempty"`
	Receipt         string                `json:"receipt"`
	Phase           string                `json:"phase"`
	PendingToolIDs  []string              `json:"pending_tool_ids,omitempty"`
}

// Proofs retain the exact client-visible transcript identity without storing
// large tool results and assistant text in Redis. Assistant block proofs also
// permit clients that split one assistant turn into adjacent messages.
type managedMessageProof struct {
	Role   string   `json:"role"`
	Digest string   `json:"digest"`
	Blocks []string `json:"blocks,omitempty"`
}

func managedProof(raw json.RawMessage) managedMessageProof {
	role := gjson.GetBytes(raw, "role").String()
	canonical := managedHistoryMessage(raw)
	if role == "assistant" {
		canonical = managedCanonicalAssistant(raw)
	}
	sum := sha256.Sum256(canonical)
	proof := managedMessageProof{Role: role, Digest: hex.EncodeToString(sum[:])}
	if role == "assistant" {
		for _, block := range managedHistoryBlocks(canonical) {
			encoded, _ := json.Marshal(block)
			blockSum := sha256.Sum256(encoded)
			proof.Blocks = append(proof.Blocks, hex.EncodeToString(blockSum[:]))
		}
	}
	return proof
}

func managedProofs(messages []json.RawMessage) []managedMessageProof {
	proofs := make([]managedMessageProof, 0, len(messages))
	for _, message := range messages {
		proofs = append(proofs, managedProof(message))
	}
	return proofs
}

func managedRecent(messages []json.RawMessage) []json.RawMessage {
	if len(messages) > 6 {
		messages = messages[len(messages)-6:]
	}
	recent := make([]json.RawMessage, 0, len(messages))
	bytes := 0
	for _, message := range messages {
		canonical := managedHistoryMessage(message)
		if len(canonical) > 16<<10 || bytes+len(canonical) > 64<<10 {
			continue
		}
		recent = append(recent, canonical)
		bytes += len(canonical)
	}
	return recent
}

type WrapperManagedAutoTurn struct {
	key                string
	token              string
	state              wrapperManagedState
	store              WrapperManagedStore
	mu                 sync.Mutex
	failed             bool
	leaseLost          chan struct{}
	loseOnce           sync.Once
	finished           bool
	stop               chan struct{}
	stopped            chan struct{}
	blocks             map[int]json.RawMessage
	toolJSON           map[int]string
	receipt            string
	terminal           bool
	stopReason         string
	resetNativeSession bool
	requestMessages    []json.RawMessage
	freshMessages      int
	historyMismatch    string
	wasReady           bool
	skipReason         string
	inputMessages      int
	normalizedMessages int
	matchedMessages    int
	mergedAssistants   int
	recentShape        string
	cleanupNative      func(context.Context, int64, string) error
	preserveBranch     bool
	priorReady         *wrapperManagedState
	handoffAttempt     string
}

// PreserveBranch prevents a request resolved against a prior snapshot from
// replacing a branch that advanced while it waited for its own turn lock.
func (t *WrapperManagedAutoTurn) PreserveBranch() { t.preserveBranch = true }

func (t *WrapperManagedAutoTurn) Decision() (string, int, int) {
	return t.skipReason, t.inputMessages, t.normalizedMessages
}

// HistoryDecision reports only structural counts. No message content is logged.
func (t *WrapperManagedAutoTurn) HistoryDecision() (int, int, string) {
	return t.matchedMessages, t.mergedAssistants, t.recentShape
}

// FreshDecision reports how many trailing user messages formed the new turn
// and, on a history mismatch, the structural shape of both sides.
func (t *WrapperManagedAutoTurn) FreshDecision() (int, string) {
	return t.freshMessages, t.historyMismatch
}

// trailingUserMessages counts the final run of user messages. Claude Code
// sends a tool continuation as tool_result followed by a separate text message
// (e.g. a context budget reminder); the API treats the run as one user turn.
func trailingUserMessages(messages []json.RawMessage) int {
	count := 0
	for index := len(messages) - 1; index >= 0 && gjson.GetBytes(messages[index], "role").String() == "user"; index-- {
		count++
	}
	return count
}

func managedMessageBlocks(raw json.RawMessage) ([]any, bool) {
	var message map[string]any
	if json.Unmarshal(raw, &message) != nil {
		return nil, false
	}
	switch content := message["content"].(type) {
	case string:
		return []any{map[string]any{"type": "text", "text": content}}, true
	case []any:
		return content, true
	}
	return nil, false
}

// mergeManagedFreshTurn joins a fresh run of user messages into the single
// user message a native Wrapper session accepts.
func mergeManagedFreshTurn(messages []json.RawMessage) (json.RawMessage, bool) {
	if len(messages) == 1 {
		return messages[0], true
	}
	merged := make([]any, 0, len(messages))
	for _, message := range messages {
		blocks, ok := managedMessageBlocks(message)
		if !ok {
			return nil, false
		}
		merged = append(merged, blocks...)
	}
	encoded, err := json.Marshal(map[string]any{"role": "user", "content": merged})
	return encoded, err == nil
}

// managedToolResultTurn builds the single tool-result message for a pending
// handoff. Text that follows the results in the same fresh run is appended to
// the last result, because the live CLI can only receive MCP tool results.
func managedToolResultTurn(messages []json.RawMessage, pendingIDs []string) (json.RawMessage, string) {
	expected := map[string]bool{}
	for _, id := range pendingIDs {
		expected[id] = true
	}
	results := make([]map[string]any, 0, len(pendingIDs))
	extra := make([]any, 0)
	for _, message := range messages {
		blocks, ok := managedMessageBlocks(message)
		if !ok {
			return nil, "invalid_tool_result"
		}
		for _, item := range blocks {
			block, ok := item.(map[string]any)
			if !ok {
				return nil, "invalid_tool_result"
			}
			switch block["type"] {
			case "tool_result":
				if len(extra) > 0 {
					// Text before a result has no faithful MCP representation.
					return nil, "pending_tool_results_changed"
				}
				id, _ := block["tool_use_id"].(string)
				if !expected[id] {
					return nil, "pending_tool_results_changed"
				}
				delete(expected, id)
				if reason := validManagedToolResult(block); reason != "" {
					return nil, reason
				}
				results = append(results, block)
			case "text":
				if _, ok := block["text"].(string); !ok {
					return nil, "invalid_tool_result"
				}
				extra = append(extra, map[string]any{"type": "text", "text": block["text"]})
			default:
				return nil, "non_text_tool_result"
			}
		}
	}
	if len(expected) > 0 || len(results) == 0 {
		return nil, "pending_tool_results_changed"
	}
	imageBytes, imageCount := 0, 0
	for _, result := range results {
		if parts, ok := result["content"].([]any); ok {
			for _, item := range parts {
				part, _ := item.(map[string]any)
				if part["type"] == "image" {
					source := part["source"].(map[string]any)
					data := source["data"].(string)
					imageBytes += base64.StdEncoding.DecodedLen(len(data)) - strings.Count(data, "=")
					imageCount++
				}
			}
		}
	}
	if imageBytes > 12<<20 || imageCount > 20 {
		return nil, "tool_result_image_budget"
	}
	if len(extra) > 0 {
		last := results[len(results)-1]
		parts := make([]any, 0, len(extra)+1)
		switch content := last["content"].(type) {
		case string:
			if content != "" {
				parts = append(parts, map[string]any{"type": "text", "text": content})
			}
		case []any:
			parts = append(parts, content...)
		}
		last["content"] = append(parts, extra...)
	}
	content := make([]any, 0, len(results))
	for _, result := range results {
		content = append(content, result)
	}
	encoded, err := json.Marshal(map[string]any{"role": "user", "content": content})
	if err != nil {
		return nil, "invalid_tool_result"
	}
	return encoded, ""
}

func validManagedToolResult(block map[string]any) string {
	switch content := block["content"].(type) {
	case string:
	case []any:
		for _, part := range content {
			p, ok := part.(map[string]any)
			if !ok {
				return "non_text_tool_result"
			}
			if p["type"] == "image" {
				if reason := validManagedImage(p); reason != "" {
					return reason
				}
				continue
			}
			if p["type"] != "text" {
				return "non_text_tool_result"
			}
			if _, ok := p["text"].(string); !ok {
				return "invalid_tool_result"
			}
		}
	default:
		return "invalid_tool_result"
	}
	if isError, exists := block["is_error"]; exists {
		if _, ok := isError.(bool); !ok {
			return "invalid_tool_result"
		}
	}
	return ""
}

func validManagedImage(block map[string]any) string {
	source, ok := block["source"].(map[string]any)
	if !ok || source["type"] != "base64" {
		return "invalid_tool_result_image"
	}
	switch source["media_type"] {
	case "image/png", "image/jpeg", "image/gif", "image/webp":
	default:
		return "invalid_tool_result_image"
	}
	data, ok := source["data"].(string)
	if !ok || len(data) == 0 || len(data)%4 != 0 || strings.ContainsAny(data, "\r\n") {
		return "invalid_tool_result_image"
	}
	if base64.StdEncoding.DecodedLen(len(data))-strings.Count(data, "=") > 10<<20 {
		return "tool_result_image_budget"
	}
	// Validate without allocating another decoded image buffer.
	if _, err := io.Copy(io.Discard, base64.NewDecoder(base64.StdEncoding, strings.NewReader(data))); err != nil {
		return "invalid_tool_result_image"
	}
	return ""
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
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if store == nil {
		return nil, errors.New("managed state store unavailable")
	}
	token, acquired, err := store.AcquireWrapperManagedLock(ctx, key, wrapperManagedLockTTL)
	if err != nil {
		return nil, err
	}
	if !acquired {
		return nil, ErrWrapperManagedBusy
	}
	turn := &WrapperManagedAutoTurn{key: key, token: token, store: store, stop: make(chan struct{}), stopped: make(chan struct{}), blocks: map[int]json.RawMessage{}, toolJSON: map[int]string{}}
	turn.leaseLost = make(chan struct{})
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
	if state := turn.state; (state.Phase != "" || state.AccountID != 0) &&
		(state.AccountID <= 0 || (state.Phase != "ready" && state.Phase != "quarantined" && state.Phase != "pending")) {
		turn.Release()
		return nil, errors.New("managed session state is invalid")
	}
	if err := ctx.Err(); err != nil {
		turn.Release()
		return nil, err
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
				t.loseOnce.Do(func() { close(t.leaseLost) })
				return
			}
		}
	}
}

// Cancel only on ownership loss, not on normal release between account attempts.
func (t *WrapperManagedAutoTurn) GuardContext(parent context.Context) context.Context {
	return guardWrapperLease(parent, t.leaseLost, t.stop)
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
	// Delete the exact old generation before losing its account/session identity.
	// A failed cleanup must remain retryable; never silently orphan a live CLI.
	if t.cleanupNative != nil && t.state.AccountID > 0 && t.state.Phase != "" {
		id := t.state.NativeSessionID
		if id == "" {
			id = WrapperManagedAutoSessionID(t.key)
		}
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		err := t.cleanupNative(cleanupCtx, t.state.AccountID, id)
		cancel()
		if err != nil {
			// Only a durably completed, idle generation can be left behind on an
			// unreachable VM. New generations have unique IDs and a fenced lease.
			// A busy response, pending tool, uncertain stream, or auth error is not safe.
			if reason != "account_changed" || t.state.Phase != "ready" || len(t.state.PendingToolIDs) != 0 ||
				t.state.Receipt == "" || !errors.Is(err, errWrapperCleanupUnavailable) {
				return fmt.Errorf("abandoned wrapper session cleanup failed: %w", err)
			}
		}
	}
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
	if len(data) > wrapperManagedMaxStateBytes && len(t.state.Proofs) > 0 {
		// Keep old, small sessions readable; compact only once their full
		// transcript would exceed the durable state limit.
		t.state.Messages = nil
		data, err = json.Marshal(t.state)
		if err != nil {
			return err
		}
	}
	if len(data) > wrapperManagedMaxStateBytes && len(t.state.Recent) > 0 {
		t.state.Recent = nil
		data, err = json.Marshal(t.state)
		if err != nil {
			return err
		}
	}
	if len(data) > wrapperManagedMaxStateBytes {
		return errors.New("managed session proof exceeds 2 MiB")
	}
	ok, err := t.store.SetWrapperManagedStateOwned(ctx, t.key, t.token, data, wrapperManagedTTL)
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("managed execution lease lost")
	}
	return nil
}

func (t *WrapperManagedAutoTurn) Quarantine() {
	t.state.Phase = "quarantined"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if t.save(ctx) != nil {
		// A compact proof may itself exceed the state cap in extreme sessions.
		// Persist a minimal quarantine marker rather than leaving a ready
		// receipt that could resume an uncertain native transcript.
		t.state = wrapperManagedState{AccountID: t.state.AccountID, NativeSessionID: t.state.NativeSessionID, Phase: "quarantined"}
		_ = t.save(ctx)
	}
}

// Reset observations while retaining the conversation lock and quarantined
// generation. Prepare will delete that generation and bootstrap a new one.
func (t *WrapperManagedAutoTurn) ResetBridgeAttempt() {
	t.blocks = map[int]json.RawMessage{}
	t.toolJSON = map[int]string{}
	t.receipt = ""
	t.terminal = false
	t.stopReason = ""
	t.resetNativeSession = false
}

func (t *WrapperManagedAutoTurn) IsReady() bool { return t.state.Phase == "ready" }

// A local capacity rejection did not advance the CLI. Retain its receipt and
// history rather than forcing the next request to rebuild a healthy session.
func (t *WrapperManagedAutoTurn) RestoreUndispatched(ctx context.Context) error {
	if t.priorReady != nil && t.priorReady.NativeSessionID == t.state.NativeSessionID && t.priorReady.AccountID == t.state.AccountID {
		t.state = *t.priorReady
		return t.save(ctx)
	}
	return nil
}

func normalizedWrapperManagedBody(body []byte, betaHeader string) ([]byte, error) {
	body = StripEmptyTextBlocks(body)
	body = FilterWebSearchHistoryBlocks(body, gjson.GetBytes(body, "model").String())
	body = stripDeferredToolCacheControl(body)
	var err error
	if system := gjson.GetBytes(body, "system"); system.Exists() {
		normalized := wrapperSystemWithoutTelemetry(json.RawMessage(system.Raw))
		if !bytes.Equal(normalized, []byte(system.Raw)) {
			body, err = sjson.SetRawBytes(body, "system", normalized)
			if err != nil {
				return nil, err
			}
		}
	}
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
			// Claude Code's built-in Edit inserts this default after executing
			// the tool. Its omission and boolean false have the same meaning.
			// Normalize only the history proof; never rewrite the wire input or
			// ignore other parameters, tool identities, or truthy/null values.
			if block["name"] == "Edit" {
				if input, ok := block["input"].(map[string]any); ok {
					if replaceAll, ok := input["replace_all"].(bool); ok && !replaceAll {
						delete(input, "replace_all")
					}
				}
			}
			filtered = append(filtered, map[string]any{"type": "tool_use", "id": block["id"], "name": block["name"], "input": block["input"]})
		default:
			return nil
		}
	}
	encoded, _ := json.Marshal(map[string]any{"role": "assistant", "content": filtered})
	return encoded
}

func equalManagedSystem(a, b json.RawMessage) bool {
	a, b = wrapperSystemWithoutTelemetry(a), wrapperSystemWithoutTelemetry(b)
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
	aRole := gjson.GetBytes(a, "role").String()
	bRole := gjson.GetBytes(b, "role").String()
	// The role is part of the transcript identity.  Comparing only the
	// canonical content would otherwise allow a user message to match a saved
	// assistant turn (or vice versa), advancing the managed prefix incorrectly.
	if aRole == "" || bRole == "" || aRole != bRole {
		return false
	}
	if bRole == "assistant" {
		left := managedCanonicalAssistant(a)
		right := managedCanonicalAssistant(b)
		return len(left) > 0 && len(right) > 0 && string(left) == string(right)
	}
	return string(managedHistoryMessage(a)) == string(managedHistoryMessage(b))
}

// matchManagedHistoryPrefix proves that the complete client history (except its
// final run of fresh user messages) is the history already executed by the CLI.
// Some clients split one assistant turn into adjacent text and tool_use
// messages. Joining only consecutive assistant blocks preserves their order and
// never treats an unexecuted user message or tool result as saved history.
func matchManagedHistoryPrefix(request, saved []json.RawMessage) (int, int, string) {
	fresh := trailingUserMessages(request)
	if fresh == 0 {
		return 0, 0, "history_length_changed"
	}
	historyLen := len(request) - fresh
	client := 0
	merged := 0
	for _, previous := range saved {
		if client >= historyLen {
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
		for client < historyLen && gjson.GetBytes(request[client], "role").String() == "assistant" {
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
	if client != historyLen {
		return client, merged, "extra_history_messages"
	}
	return client, merged, ""
}

func matchManagedHistoryProofs(request []json.RawMessage, saved []managedMessageProof) (int, int, string) {
	fresh := trailingUserMessages(request)
	if fresh == 0 {
		return 0, 0, "history_length_changed"
	}
	historyLen := len(request) - fresh
	client, merged := 0, 0
	for _, expected := range saved {
		if client >= historyLen {
			return client, merged, "history_length_changed"
		}
		actual := managedProof(request[client])
		if actual.Role == expected.Role && actual.Digest == expected.Digest {
			client++
			continue
		}
		if expected.Role != "assistant" || actual.Role != "assistant" || len(expected.Blocks) == 0 {
			return client, merged, "history_changed"
		}
		start, blockIndex := client, 0
		for client < historyLen && gjson.GetBytes(request[client], "role").String() == "assistant" {
			part := managedProof(request[client])
			if len(part.Blocks) == 0 || blockIndex+len(part.Blocks) > len(expected.Blocks) {
				return client, merged, "history_changed"
			}
			for _, digest := range part.Blocks {
				if digest != expected.Blocks[blockIndex] {
					return client, merged, "history_changed"
				}
				blockIndex++
			}
			client++
			if blockIndex == len(expected.Blocks) {
				merged += client - start - 1
				break
			}
		}
		if blockIndex != len(expected.Blocks) {
			return client, merged, "history_changed"
		}
	}
	if client != historyLen {
		return client, merged, "extra_history_messages"
	}
	return client, merged, ""
}

// Prepare accepts a complete downstream transcript. The initial managed turn
// seeds the native session with validated history; later turns send only the
// fresh user message.
func (t *WrapperManagedAutoTurn) Prepare(ctx context.Context, accountID int64, body []byte, c *gin.Context) ([]byte, bool, error) {
	if promptID := c.GetHeader("X-Claude-Code-Prompt-Id"); promptID != "" {
		cancelled, err := t.store.GetWrapperManagedState(ctx, wrapperCancelledPromptKey(t.key, promptID))
		if err != nil {
			return nil, false, err
		}
		if len(cancelled) > 0 {
			return nil, true, &WrapperManagedPayloadError{Status: 409, Reason: "execution was explicitly cancelled; submit a new prompt"}
		}
	}
	cancelledGeneration := false
	if t.state.NativeSessionID != "" {
		cancelled, err := t.store.GetWrapperManagedState(ctx, wrapperCancelledGenerationKey(t.key, t.state.NativeSessionID))
		if err != nil {
			return nil, false, err
		}
		if len(cancelled) > 0 {
			var marker struct {
				PromptID string `json:"prompt_id"`
			}
			if json.Unmarshal(cancelled, &marker) != nil {
				return nil, false, errors.New("invalid cancelled generation marker")
			}
			if marker.PromptID != "" && marker.PromptID == c.GetHeader("X-Claude-Code-Prompt-Id") {
				return nil, true, &WrapperManagedPayloadError{Status: 409, Reason: "execution was explicitly cancelled; submit a new prompt"}
			}
			cancelledGeneration = marker.PromptID != "" && c.GetHeader("X-Claude-Code-Prompt-Id") != "" && c.GetHeader("X-Claude-Code-Request-Class") == "main"
			t.state.Phase = "quarantined"
		}
	}
	t.priorReady = nil
	if t.state.Phase == "ready" {
		data, err := json.Marshal(t.state)
		if err != nil {
			return nil, false, err
		}
		var saved wrapperManagedState
		if err := json.Unmarshal(data, &saved); err != nil {
			return nil, false, err
		}
		t.priorReady = &saved
	}
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
		// The complete downstream transcript can seed a new native session on
		// the selected replacement VM. A fresh tool_result remains disallowed.
	}
	resyncReason := ""
	var bashCorrections []managedBashCorrection
	if t.state.Phase != "" && t.state.Phase != "ready" {
		resyncReason = "incomplete_previous_turn"
	} else if t.state.Phase == "ready" {
		switch {
		case t.state.Model != request.Model || !equalManagedSystem(t.state.System, request.System):
			resyncReason = "model_or_system_changed"
		case t.state.Receipt == "":
			resyncReason = "receipt_unavailable"
		default:
			if len(t.state.Proofs) > 0 {
				t.matchedMessages, t.mergedAssistants, resyncReason = matchManagedHistoryProofs(request.Messages, t.state.Proofs)
			} else {
				t.matchedMessages, t.mergedAssistants, resyncReason = matchManagedHistoryPrefix(request.Messages, t.state.Messages)
			}
			if resyncReason == "history_changed" {
				if corrections, ok := t.reconcilePendingBash(request.Messages, c.GetHeader("User-Agent")); ok {
					bashCorrections = corrections
					resyncReason = ""
					t.matchedMessages = len(request.Messages) - trailingUserMessages(request.Messages)
					t.historyMismatch = "pending_bash_cd_reconciled_with_result_notice"
				}
			}
			if resyncReason == "history_changed" && t.matchedMessages < len(request.Messages) && t.matchedMessages < len(t.state.Messages) {
				t.historyMismatch = "saved=" + managedRecentMessageShape(t.state.Messages[t.matchedMessages:t.matchedMessages+1]) +
					" request=" + managedRecentMessageShape(request.Messages[t.matchedMessages:t.matchedMessages+1])
			}
		}
	}
	if resyncReason != "" {
		if t.preserveBranch && !cancelledGeneration {
			return nil, false, &WrapperManagedPayloadError{Status: 409, Reason: "managed branch advanced or changed; refusing to replace its history"}
		}
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
	var recoveredHistory []json.RawMessage
	if (t.preserveBranch || cancelledGeneration) && t.state.Phase == "" {
		if history, ok := managedBranchRecoveryHistory(request.Messages); ok {
			recoveredHistory = history
			t.skipReason = "branch_complete_tool_history_recovered"
		}
	}
	if t.state.Phase == "" && recoveredHistory == nil && !validManagedBootstrapHistory(request.Messages) {
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
	if t.state.Phase == "" && recoveredHistory == nil && hasManagedToolResult(fresh) {
		t.skipReason = "pending_tool_result_without_handoff"
		return body, false, nil
	}
	// A resumed session receives the whole trailing user run as one turn; a new
	// session is seeded with everything before the final message.
	t.freshMessages = 1
	if t.state.Phase == "ready" {
		t.freshMessages = trailingUserMessages(request.Messages)
	}
	freshRun := request.Messages[len(request.Messages)-t.freshMessages:]
	if t.state.Phase == "ready" && len(t.state.PendingToolIDs) == 0 {
		for _, message := range freshRun {
			if hasManagedToolResult(message) {
				if err := t.abandonNativeSession(ctx, "unexpected_tool_result"); err != nil {
					return nil, false, err
				}
				t.skipReason = "unexpected_tool_result"
				return body, false, nil
			}
		}
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
		bootstrap := wrapperManagedState{AccountID: accountID, NativeSessionID: nativeSessionID, Model: request.Model, System: request.System, Messages: prefix, Proofs: managedProofs(prefix), Recent: managedRecent(prefix), Phase: "pending"}
		t.state = bootstrap
	} else {
		// Keep the legacy ID for sessions created before native_session_id was
		// persisted. New sessions always carry a unique generation suffix.
		if t.state.NativeSessionID == "" {
			t.state.NativeSessionID = WrapperManagedAutoSessionID(t.key)
		}
	}
	freshTurn := fresh
	if len(t.state.PendingToolIDs) > 0 {
		var reason string
		freshTurn, reason = managedToolResultTurn(freshRun, t.state.PendingToolIDs)
		if reason != "" {
			if t.preserveBranch {
				return nil, true, &WrapperManagedPayloadError{Status: 400, Reason: reason}
			}
			if reason == "invalid_tool_result_image" || reason == "tool_result_image_budget" || reason == "non_text_tool_result" {
				status := 400
				if reason == "tool_result_image_budget" {
					status = 413
				}
				return nil, true, &WrapperManagedPayloadError{Status: status, Reason: reason}
			}
			if err := t.abandonNativeSession(ctx, reason); err != nil {
				return nil, false, err
			}
			t.skipReason = reason
			return body, false, nil
		}
	} else if t.state.Phase == "ready" {
		merged, ok := mergeManagedFreshTurn(freshRun)
		if !ok {
			if err := t.abandonNativeSession(ctx, "invalid_fresh_turn"); err != nil {
				return nil, false, err
			}
			t.skipReason = "invalid_fresh_turn"
			return body, false, nil
		}
		freshTurn = merged
	}
	wasReady := t.state.Phase == "ready"
	if len(bashCorrections) > 0 {
		freshTurn = managedAnnotateBashResult(freshTurn, bashCorrections)
	}
	forwardMessages := append(append([]byte("["), freshTurn...), ']')
	if !wasReady {
		forwardMessages = []byte(gjson.GetBytes(normalized, "messages").Raw)
		if recoveredHistory != nil {
			forwardMessages, err = json.Marshal(recoveredHistory)
			if err != nil {
				return nil, true, err
			}
		}
	}
	updated, err := sjson.SetRawBytes(normalized, "messages", forwardMessages)
	if err != nil {
		return nil, true, err
	}
	// Match the baseline Wrapper envelope budget before committing pending
	// state. Full client history may be larger; only the forwarded turn counts.
	if len(updated) > 20<<20 {
		return nil, true, &WrapperManagedPayloadError{Status: 413, Reason: "managed_wrapper_request_exceeds_20_mib"}
	}
	t.wasReady = wasReady
	if len(bashCorrections) > 0 {
		// Advance only the client-side comparison ledger after all validation.
		// The upstream retains its original call plus the explicit result notice.
		returned := request.Messages[len(request.Messages)-t.freshMessages-1]
		if n := len(t.state.Proofs); n > 0 {
			t.state.Proofs[n-1] = managedProof(returned)
		}
		if n := len(t.state.Messages); n > 0 {
			t.state.Messages[n-1] = managedHistoryMessage(returned)
		}
	}
	t.requestMessages = append([]json.RawMessage(nil), request.Messages...)
	t.state.Phase = "pending"
	if err := t.save(ctx); err != nil {
		return nil, true, err
	}
	c.Request.Header.Set("X-Wrapper-Session-Mode", "managed")
	var attempt [16]byte
	if _, err := rand.Read(attempt[:]); err != nil {
		return nil, true, err
	}
	t.handoffAttempt = hex.EncodeToString(attempt[:])
	c.Set("wrapper_handoff_attempt", t.handoffAttempt)
	c.Set("wrapper_managed_state_key", t.key)
	c.Request.Header.Set("X-Wrapper-Auto-Managed", "1")
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
		t.resetNativeSession = gjson.GetBytes(data, "wrapper_session_reset").Bool()
	case "message_delta":
		t.stopReason = gjson.GetBytes(data, "delta.stop_reason").String()
	}
}

func (t *WrapperManagedAutoTurn) Commit(ctx context.Context) error {
	t.mu.Lock()
	if !t.terminal || (t.failed && !(t.resetNativeSession && t.stopReason == "max_tokens")) {
		t.mu.Unlock()
		return errors.New("managed stream did not complete safely")
	}
	if t.resetNativeSession {
		t.mu.Unlock()
		// The client received a complete truncation, but the CLI process was
		// stopped before recovery could advance its hidden native history.
		// Never acknowledge/resume that transcript; Prepare rebuilds from the
		// next complete client history under a new native session ID.
		t.Quarantine()
		return nil
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
	fresh := t.freshMessages
	if fresh < 1 || fresh > len(t.requestMessages) {
		fresh = 1
	}
	previous := append([]json.RawMessage(nil), t.state.Messages...)
	proofs := append([]managedMessageProof(nil), t.state.Proofs...)
	if len(proofs) == 0 && len(previous) > 0 {
		proofs = managedProofs(previous)
	}
	for _, message := range t.requestMessages[len(t.requestMessages)-fresh:] {
		canonical := managedHistoryMessage(message)
		if t.state.Messages != nil {
			previous = append(previous, canonical)
		}
		proofs = append(proofs, managedProof(canonical))
	}
	if t.state.Messages != nil {
		t.state.Messages = append(previous, assistant)
	}
	t.state.Proofs = append(proofs, managedProof(assistant))
	t.state.Recent = managedRecent(append(append([]json.RawMessage(nil), t.requestMessages...), assistant))
	t.state.Receipt = receipt
	t.state.Phase = "ready"
	return t.save(ctx)
}
