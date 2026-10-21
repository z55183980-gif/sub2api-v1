package service

import (
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/sjson"
)

// PrepareNativeFork verifies lineage without saving, acknowledging, retiring or
// advancing the parent. The caller must retain this turn's lock until HTTP ends.
func (t *WrapperManagedAutoTurn) PrepareNativeFork(body []byte, c *gin.Context) ([]byte, bool, error) {
	if t.state.Phase != "ready" || t.state.Receipt == "" || t.state.NativeSessionID == "" || len(t.state.PendingToolIDs) > 0 {
		return body, false, nil
	}
	normalized, err := normalizedWrapperManagedBody(body, c.GetHeader("anthropic-beta"))
	if err != nil {
		return nil, false, err
	}
	var request struct {
		Model    string            `json:"model"`
		System   json.RawMessage   `json:"system"`
		Messages []json.RawMessage `json:"messages"`
	}
	if err = json.Unmarshal(normalized, &request); err != nil {
		return nil, false, err
	}
	if request.Model != t.state.Model || !equalManagedSystem(t.state.System, request.System) {
		return body, false, nil
	}
	var matched int
	var reason string
	if len(t.state.Proofs) > 0 {
		matched, _, reason = matchManagedHistoryProofs(request.Messages, t.state.Proofs)
	} else {
		matched, _, reason = matchManagedHistoryPrefix(request.Messages, t.state.Messages)
	}
	if reason != "" || matched == 0 || matched >= len(request.Messages) {
		return body, false, nil
	}
	fresh := request.Messages[matched:]
	if trailingUserMessages(fresh) != len(fresh) {
		return body, false, nil
	}
	for _, message := range fresh {
		blocks, ok := managedMessageBlocks(message)
		if !ok {
			return body, false, nil
		}
		for _, item := range blocks {
			block, ok := item.(map[string]any)
			if !ok || block["type"] != "text" {
				return body, false, nil
			}
		}
	}
	merged, ok := mergeManagedFreshTurn(fresh)
	if !ok {
		return body, false, nil
	}
	updated, err := sjson.SetRawBytes(normalized, "messages", append(append([]byte{'['}, merged...), ']'))
	if err != nil {
		return nil, false, err
	}
	// Recaps have no client tool execution. Retain the native parent's prefix.
	for _, field := range []string{"tools", "tool_choice", "metadata.mcp_servers"} {
		updated, err = sjson.DeleteBytes(updated, field)
		if err != nil {
			return nil, false, err
		}
	}
	c.Request.Header.Set("X-Wrapper-Fork-Parent", t.state.NativeSessionID)
	c.Request.Header.Set("X-Wrapper-Fork-Turn-Id", t.state.Receipt)
	c.Request.Header.Set("X-Wrapper-Auto-Managed", "1")
	return updated, true, nil
}
