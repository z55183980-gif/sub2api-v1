package handler

import (
	"os"
	"strconv"
	"strings"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

// Isolate before managed lookup, locking, replay and sticky routing. A recap
// must never commit its user/assistant pair to the parent's history proofs.
func isolateWrapperRecap(c *gin.Context, keyID int64, body []byte, metadata string) string {
	if os.Getenv("SUB2API_WRAPPER_RECAP_STATELESS") == "0" ||
		!service.AutoWrapperManagedEnabledForKey(keyID) || c.GetHeader("X-Wrapper-Session-Mode") != "" {
		return ""
	}
	// Respect an explicit main/subagent classification; the fallback covers
	// older clients missing the auxiliary class, plus the named recap class.
	if kind := c.GetHeader("X-Claude-Code-Request-Class"); kind != "" && kind != "session_recap" {
		return ""
	}
	if c.GetHeader("X-Claude-Code-Request-Class") != "session_recap" && !service.IsClaudeSessionRecapRequest(body, c.GetHeader("User-Agent")) {
		return ""
	}
	uid := service.ParseMetadataUserID(metadata)
	if uid == nil || uid.SessionID == "" {
		return ""
	}
	identity := service.WrapperManagedConversationID(uid.SessionID, c.GetHeader("X-Claude-Code-Agent-Id"))
	identity = service.WrapperClientScope(c.GetHeader("X-Wrapper-Client-Scope")) + "\x00" + identity
	routing := "claude-recap-" + service.WrapperManagedAutoKey(keyID, identity)
	c.Request.Header.Set("X-Wrapper-Session-Mode", "stateless")
	// With no CLI session header, the upstream metadata synchronizer cannot
	// reintroduce the parent owner. Stateless wrappers allocate their own UUID.
	for _, header := range []string{"X-Claude-Code-Session-Id", "X-Wrapper-Session-Id", "X-Wrapper-Ack-Turn-Id", "X-Wrapper-Auto-Managed", "X-Idempotency-Key"} {
		c.Request.Header.Del(header)
	}
	return routing
}

func nativeForkAccountEnabled(id int64) bool {
	for _, v := range strings.Split(os.Getenv("SUB2API_WRAPPER_NATIVE_FORK_ACCOUNT_IDS"), ",") {
		if strings.TrimSpace(v) == strconv.FormatInt(id, 10) {
			return id > 0
		}
	}
	return false
}

func recapParentConversation(c *gin.Context, metadata string) string {
	uid := service.ParseMetadataUserID(metadata)
	if uid == nil || uid.SessionID == "" {
		return ""
	}
	id := service.WrapperManagedConversationID(uid.SessionID, c.GetHeader("X-Claude-Code-Agent-Id"))
	if scope := service.WrapperClientScope(c.GetHeader("X-Wrapper-Client-Scope")); scope != "" {
		id = "scoped-" + service.HashUsageRequestPayload([]byte(scope+"\x00"+id))
	}
	return id
}
