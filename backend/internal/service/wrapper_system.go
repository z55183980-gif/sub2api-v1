package service

import (
	"encoding/json"
	"strings"
)

// Match the wrapper's system-prompt translation, including saved generations
// created before telemetry filtering. Only standalone text blocks are metadata;
// strings and multiline policy text remain part of the system identity.
func wrapperSystemWithoutTelemetry(raw json.RawMessage) json.RawMessage {
	var blocks []json.RawMessage
	if json.Unmarshal(raw, &blocks) != nil {
		return raw
	}
	filtered := make([]json.RawMessage, 0, len(blocks))
	for _, block := range blocks {
		var b struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}
		if json.Unmarshal(block, &b) == nil && b.Type == "text" &&
			strings.HasPrefix(b.Text, "x-anthropic-billing-header:") && !strings.ContainsAny(b.Text, "\r\n") {
			continue
		}
		filtered = append(filtered, block)
	}
	if len(filtered) == len(blocks) {
		return raw
	}
	normalized, err := json.Marshal(filtered)
	if err != nil {
		return raw
	}
	return normalized
}
