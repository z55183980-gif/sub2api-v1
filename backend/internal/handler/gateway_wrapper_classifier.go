package handler

import (
	"os"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// Run before sticky lookup and managed locking. Permission classification shares
// the parent's metadata.session_id, but must never consume its pending tools,
// replace its receipt, or reset its native CLI. Keep the request body unchanged.
func isolateWrapperClassifier(c *gin.Context, keyID int64, body []byte, metadata string) string {
	if os.Getenv("SUB2API_WRAPPER_CLASSIFIER_STATELESS") == "0" ||
		!service.AutoWrapperManagedEnabledForKey(keyID) || c.GetHeader("X-Wrapper-Session-Mode") != "" ||
		!service.IsClaudeAutoClassifierRequest(body, c.GetHeader("User-Agent")) {
		return ""
	}
	uid := service.ParseMetadataUserID(metadata)
	if uid == nil || uid.SessionID == "" {
		return ""
	}
	identity := service.WrapperManagedConversationID(uid.SessionID, c.GetHeader("X-Claude-Code-Agent-Id"))
	routing := "claude-classifier-" + service.WrapperManagedAutoKey(keyID, identity)
	c.Request.Header.Set("X-Wrapper-Session-Mode", "stateless")
	for _, header := range []string{"X-Wrapper-Session-Id", "X-Wrapper-Ack-Turn-Id", "X-Wrapper-Auto-Managed", "X-Idempotency-Key"} {
		c.Request.Header.Del(header)
	}
	return routing
}
