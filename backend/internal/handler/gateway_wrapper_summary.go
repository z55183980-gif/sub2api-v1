package handler

import (
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"os"
)

func isolateWrapperSummary(c *gin.Context, keyID int64, body []byte, metadata string) (string, []byte, error) {
	if os.Getenv("SUB2API_WRAPPER_SUMMARY_STATELESS") == "0" || !service.AutoWrapperManagedEnabledForKey(keyID) || c.GetHeader("X-Wrapper-Session-Mode") != "" || !service.IsClaudeAgentSummaryRequest(body, c.GetHeader("User-Agent"), c.GetHeader("X-Claude-Code-Agent-Id")) {
		return "", body, nil
	}
	uid := service.ParseMetadataUserID(metadata)
	if uid == nil || uid.SessionID == "" {
		return "", body, nil
	}
	updated, err := service.PrepareClaudeAgentSummary(body)
	if err != nil {
		return "", body, err
	}
	identity := service.WrapperManagedConversationID(uid.SessionID, c.GetHeader("X-Claude-Code-Agent-Id"))
	routing := "claude-summary-" + service.WrapperManagedAutoKey(keyID, identity)
	c.Request.Header.Set("X-Wrapper-Session-Mode", "stateless")
	for _, header := range []string{"X-Wrapper-Session-Id", "X-Wrapper-Ack-Turn-Id", "X-Wrapper-Auto-Managed", "X-Idempotency-Key"} {
		c.Request.Header.Del(header)
	}
	return routing, updated, nil
}
