package service

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// WrapperManagedConversationID identifies a CLI conversation, not the whole
// CLI workspace session. Claude Code 2.1.300 sends the parent's session_id for
// every subagent and supplies the stable child ID in X-Claude-Code-Agent-Id.
// Preserve the main conversation's existing keys and receipts during upgrades.
// JSON framing prevents ambiguous session/agent concatenations.
func WrapperManagedConversationID(sessionID, agentID string) string {
	if agentID == "" || sessionID == "" {
		return sessionID
	}
	identity, _ := json.Marshal([]string{"claude-agent-v1", sessionID, agentID})
	sum := sha256.Sum256(identity)
	return "claude-agent-" + hex.EncodeToString(sum[:])
}
