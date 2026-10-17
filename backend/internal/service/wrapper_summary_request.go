package service

import (
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"strings"
)

// This exact instruction and its position are from Claude Code 2.1.300's
// agent_summary fork. This is a routing hint, never authentication.
const claudeAgentSummaryInstruction = `Describe your most recent action in 3-5 words using present tense (-ing). Name the file or function, not the branch. Do not use tools.

Good: "Reading runAgent.ts"
Good: "Fixing null check in validate.ts"
Good: "Running auth module tests"
Good: "Adding retry logic to fetchUser"

Bad (past tense): "Analyzed the branch diff"
Bad (too vague): "Investigating the issue"
Bad (too long): "Reviewing full branch diff and AgentTool.tsx integration"
Bad (branch name): "Analyzed adam/background-summary branch diff"`

func IsClaudeAgentSummaryRequest(body []byte, userAgent, agentID string) bool {
	if agentID == "" || !strings.HasPrefix(userAgent, "claude-cli/") || !gjson.ValidBytes(body) {
		return false
	}
	root := gjson.ParseBytes(body)
	if root.Get("metadata.mcp_servers").Exists() || root.Get("mcp_servers").Exists() {
		return false
	}
	if choice := root.Get("tool_choice"); choice.Exists() && choice.Get("type").String() != "none" {
		return false
	}
	if !isClaudeSummaryAgentSystem(root.Get("system")) {
		return false
	}
	messages := root.Get("messages").Array()
	i := len(messages) - 1
	for i >= 0 && messages[i].Get("role").String() == "system" {
		i--
	}
	if i < 1 || messages[i].Get("role").String() != "user" {
		return false
	}
	content := messages[i].Get("content")
	if !content.IsArray() {
		return false
	}
	blocks := content.Array()
	if len(blocks) < 2 || blocks[len(blocks)-1].Get("type").String() != "text" || !isClaudeAgentSummaryInstruction(blocks[len(blocks)-1].Get("text").String()) {
		return false
	}
	if messages[i-1].Get("role").String() != "assistant" {
		return false
	}
	calls := map[string]bool{}
	for _, b := range messages[i-1].Get("content").Array() {
		if b.Get("type").String() == "tool_use" && b.Get("id").String() != "" {
			calls[b.Get("id").String()] = true
		}
	}
	for _, b := range blocks[:len(blocks)-1] {
		// A fork also appends this instruction to an ordinary user text turn,
		// including the CLI's automatic continuation after an output limit.
		if b.Get("type").String() == "text" && b.Get("text").Type == gjson.String {
			continue
		}
		id := b.Get("tool_use_id").String()
		if b.Get("type").String() != "tool_result" || !calls[id] {
			return false
		}
		delete(calls, id)
	}
	return len(calls) == 0
}

// Custom CLI agents replace the built-in agent prompt. Recognize the observed
// SDK identity and billing marker together; neither alone identifies a summary.
func isClaudeSummaryAgentSystem(value gjson.Result) bool {
	system, ok := titleRequestText(value)
	if !ok {
		return false
	}
	if strings.Contains(system, "You are an agent for Claude Code, Anthropic's official CLI for Claude.") {
		return true
	}
	if !value.IsArray() {
		return false
	}
	identity, subagent := false, false
	for _, block := range value.Array() {
		text := block.Get("text").String()
		if text == "You are a Claude agent, built on Anthropic's Claude Agent SDK." {
			identity = true
		}
		if strings.HasPrefix(text, "x-anthropic-billing-header:") {
			for _, field := range strings.Split(strings.TrimPrefix(text, "x-anthropic-billing-header:"), ";") {
				if strings.TrimSpace(field) == "cc_is_subagent=true" {
					subagent = true
				}
			}
		}
	}
	return identity && subagent
}

// Preserve the complete history for a fresh CLI, but enforce the summary's
// explicit no-tools instruction through the wrapper's existing tool control.
func PrepareClaudeAgentSummary(body []byte) ([]byte, error) {
	return sjson.SetBytes(body, "tool_choice", map[string]string{"type": "none"})
}

// Subsequent native summaries insert the previously displayed status between
// the fixed instruction and examples. Match its framing, not its content.
func isClaudeAgentSummaryInstruction(text string) bool {
	if text == claudeAgentSummaryInstruction {
		return true
	}
	parts := strings.SplitN(claudeAgentSummaryInstruction, "\n\n", 2)
	prefix := parts[0] + "\n\nPrevious: \""
	suffix := "\" — say something NEW.\n\n" + parts[1]
	if !strings.HasPrefix(text, prefix) || !strings.HasSuffix(text, suffix) {
		return false
	}
	previous := strings.TrimSuffix(strings.TrimPrefix(text, prefix), suffix)
	return len(previous) > 0 && len(previous) <= 1024 && !strings.ContainsAny(previous, "\r\n")
}
