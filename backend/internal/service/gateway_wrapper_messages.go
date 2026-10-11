package service

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// normalizeWrapperMessages adapts mid-conversation system control messages
// emitted by some clients to the CLI Wrapper's user/assistant-only input.
func normalizeWrapperMessages(body []byte) ([]byte, error) {
	messages := gjson.GetBytes(body, "messages")
	if !messages.IsArray() {
		return body, nil
	}
	var rawMessages []json.RawMessage
	if err := json.Unmarshal([]byte(messages.Raw), &rawMessages); err != nil {
		return nil, err
	}
	changed := false
	result := make([]json.RawMessage, 0, len(rawMessages))
	for _, message := range rawMessages {
		if gjson.GetBytes(message, "role").String() != "system" {
			result = append(result, message)
			continue
		}
		changed = true
		content := gjson.GetBytes(message, "content")
		var textParts []string
		switch {
		case !content.Exists() || content.Type == gjson.Null:
		case content.Type == gjson.String:
			textParts = append(textParts, content.String())
		case content.IsArray():
			for _, block := range content.Array() {
				if block.Get("type").String() != "text" || block.Get("text").Type != gjson.String {
					return nil, fmt.Errorf("Wrapper cannot translate a non-text system message")
				}
				textParts = append(textParts, block.Get("text").String())
			}
		default:
			return nil, fmt.Errorf("Wrapper cannot translate system message content")
		}
		text := strings.TrimSpace(strings.Join(textParts, "\n"))
		if text == "" {
			continue
		}
		converted, err := json.Marshal(map[string]any{
			"role":    "user",
			"content": "<system_instruction>\n" + text + "\n</system_instruction>",
		})
		if err != nil {
			return nil, err
		}
		result = append(result, converted)
	}
	if !changed {
		return body, nil
	}
	if len(result) == 0 {
		return nil, fmt.Errorf("Wrapper request has no user or assistant messages")
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	return sjson.SetRawBytes(body, "messages", encoded)
}
