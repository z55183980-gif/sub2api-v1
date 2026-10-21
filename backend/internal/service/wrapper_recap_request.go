package service

import (
	"strings"

	"github.com/tidwall/gjson"
)

// Exact auxiliary instruction observed in Claude Code's session recap fork.
// This is a compatibility routing hint, never an authentication decision.
const claudeSessionRecapInstruction = "The user stepped away and is coming back. Recap in under 40 words, 1-2 plain sentences, no markdown. Lead with the overall goal and current task, then the one next action. Skip root-cause narrative, fix internals, secondary to-dos, and em-dash tangents."

func IsClaudeSessionRecapRequest(body []byte, userAgent string) bool {
	if !strings.HasPrefix(userAgent, "claude-cli/") || !gjson.ValidBytes(body) {
		return false
	}
	messages := gjson.GetBytes(body, "messages")
	if !messages.IsArray() {
		return false
	}
	turns := messages.Array()
	i := len(turns) - 1
	for i >= 0 && isClaudeSummaryTokenNotice(turns[i]) {
		i--
	}
	if i < 0 || turns[i].Get("role").String() != "user" {
		return false
	}
	text, ok := titleRequestText(turns[i].Get("content"))
	return ok && strings.TrimSpace(text) == claudeSessionRecapInstruction
}
