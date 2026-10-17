package handler

import (
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestWrapperJSONReplayClientParity(t *testing.T) {
	t.Setenv("SUB2API_WRAPPER_DESKTOP_BRANCHES", "0")
	t.Setenv("SUB2API_WRAPPER_JSON_REPLAY_DISABLED", "0")
	for _, ua := range []string{"claude-cli/2.1.299 (external, cli)", "claude-cli/2.1.301 (external, sdk-cli)", "claude-cli/2.1.299 (external, claude-desktop-3p)"} {
		for _, agent := range []string{"", "child"} {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
			c.Request.Header.Set("User-Agent", ua)
			c.Request.Header.Set("X-Claude-Code-Agent-Id", agent)
			if !wrapperJSONReplayEnabled(c, "conversation", false) {
				t.Fatalf("JSON replay excluded %s agent=%s", ua, agent)
			}
			if wrapperJSONReplayEnabled(c, "conversation", true) || wrapperJSONReplayEnabled(c, "", false) {
				t.Fatal("streaming or non-managed traffic entered replay")
			}
			t.Setenv("SUB2API_WRAPPER_JSON_REPLAY_DISABLED", "1")
			if wrapperJSONReplayEnabled(c, "conversation", false) {
				t.Fatal("disable ignored")
			}
			t.Setenv("SUB2API_WRAPPER_JSON_REPLAY_DISABLED", "0")
		}
	}
}
