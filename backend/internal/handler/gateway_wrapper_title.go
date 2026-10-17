package handler

import (
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"os"
)

// Apply before sticky lookup and managed state lookup, so title retries and
// failover cannot read, overwrite or quarantine the main conversation.
func isolateWrapperTitle(c *gin.Context, keyID int64, body []byte, metadata string) string {
	if os.Getenv("SUB2API_WRAPPER_TITLE_STATELESS") == "0" ||
		!service.AutoWrapperManagedEnabledForKey(keyID) || c.GetHeader("X-Wrapper-Session-Mode") != "" ||
		c.GetHeader("X-Claude-Code-Agent-Id") != "" ||
		!service.IsClaudeSessionTitleRequest(body, c.GetHeader("User-Agent")) {
		return ""
	}
	uid := service.ParseMetadataUserID(metadata)
	if uid == nil || uid.SessionID == "" {
		return ""
	}
	// Domain separation also includes the API key, unlike the original raw CLI
	// metadata session. Keep downstream identity and the model payload intact.
	routing := "claude-title-" + service.WrapperManagedAutoKey(keyID, uid.SessionID)
	c.Request.Header.Set("X-Wrapper-Session-Mode", "stateless")
	for _, header := range []string{"X-Wrapper-Session-Id", "X-Wrapper-Ack-Turn-Id", "X-Wrapper-Auto-Managed", "X-Idempotency-Key"} {
		c.Request.Header.Del(header)
	}
	return routing
}
