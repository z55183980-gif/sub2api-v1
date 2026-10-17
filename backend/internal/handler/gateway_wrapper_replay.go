package handler

import (
	"os"
	"strings"

	"github.com/gin-gonic/gin"
)

// Replay applies to automatic Claude managed JSON requests independently of
// legacy desktop branch routing. Explicit/stateless and auxiliary requests
// have no automatic conversation ID and remain outside this path.
func wrapperJSONReplayEnabled(c *gin.Context, conversationID string, streaming bool) bool {
	return conversationID != "" && !streaming &&
		strings.HasPrefix(c.GetHeader("User-Agent"), "claude-cli/") &&
		os.Getenv("SUB2API_WRAPPER_JSON_REPLAY_DISABLED") != "1"
}
