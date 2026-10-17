package service

import (
	"github.com/tidwall/gjson"
	"strings"
)

// IsClaudeSessionTitleRequest recognizes the independent title call observed in
// Claude Code 2.1.300. Never classify ordinary text/JSON requests by size alone.
// This is a routing hint, not authentication or a permission decision.
func IsClaudeSessionTitleRequest(body []byte, userAgent string) bool {
	if !strings.HasPrefix(userAgent, "claude-cli/") || !gjson.ValidBytes(body) {
		return false
	}
	root := gjson.ParseBytes(body)
	tools := root.Get("tools")
	if tools.Exists() && (!tools.IsArray() || len(tools.Array()) != 0) {
		return false
	}
	if root.Get("metadata.mcp_servers").Exists() || root.Get("tool_choice").Exists() {
		return false
	}
	messages := root.Get("messages")
	if !messages.IsArray() || len(messages.Array()) != 1 || messages.Get("0.role").String() != "user" {
		return false
	}
	text, ok := titleRequestText(messages.Get("0.content"))
	if !ok || !strings.HasPrefix(strings.TrimSpace(text), "<session>") || !strings.Contains(text, "</session>") {
		return false
	}
	system, ok := titleRequestText(root.Get("system"))
	if !ok || !strings.Contains(system, "You are naming a coding session so the user can pick it out of a long list of sessions.") ||
		!strings.Contains(system, `Return JSON with a single "title" field.`) {
		return false
	}
	format := root.Get("output_config.format")
	schema := format.Get("schema")
	props := schema.Get("properties")
	required := schema.Get("required")
	return format.Get("type").String() == "json_schema" && schema.Get("type").String() == "object" &&
		props.IsObject() && len(props.Map()) == 1 && props.Get("title.type").String() == "string" &&
		required.IsArray() && len(required.Array()) == 1 && required.Get("0").String() == "title" &&
		schema.Get("additionalProperties").Type == gjson.False
}

func titleRequestText(value gjson.Result) (string, bool) {
	if value.Type == gjson.String {
		return value.String(), true
	}
	if !value.IsArray() || len(value.Array()) == 0 {
		return "", false
	}
	var texts []string
	for _, block := range value.Array() {
		if block.Get("type").String() != "text" || block.Get("text").Type != gjson.String {
			return "", false
		}
		texts = append(texts, block.Get("text").String())
	}
	return strings.Join(texts, "\n"), true
}
