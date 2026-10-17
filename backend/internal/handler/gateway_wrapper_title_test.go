package handler

import (
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"net/http/httptest"
	"os"
	"testing"
)

func TestTitleIsolationPrecedesManagedAndStickyState(t *testing.T) {
	t.Setenv(service.AutoWrapperManagedEnv, "1")
	t.Setenv(service.AutoWrapperManagedKeyIDsEnv, "1,2")
	b, err := os.ReadFile("../service/testdata/wrapper_title/title.json")
	if err != nil {
		t.Fatal(err)
	}
	metadata := `{"device_id":"test-device","session_id":"title-parent"}`
	for _, tc := range []struct {
		name, mode, agent, disabled string
		key                         int64
		isolate                     bool
	}{
		{name: "title", key: 1, isolate: true}, {name: "other key", key: 9}, {name: "explicit managed", mode: "managed", key: 1}, {name: "explicit stateless", mode: "stateless", key: 1}, {name: "agent", agent: "child", key: 1}, {name: "rollback", disabled: "0", key: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SUB2API_WRAPPER_TITLE_STATELESS", tc.disabled)
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
			c.Request.Header.Set("User-Agent", "claude-cli/2.1.300")
			c.Request.Header.Set("X-Wrapper-Session-Mode", tc.mode)
			c.Request.Header.Set("X-Claude-Code-Agent-Id", tc.agent)
			c.Request.Header.Set("X-Wrapper-Ack-Turn-Id", "main-receipt")
			c.Request.Header.Set("X-Wrapper-Session-Id", "main-state")
			routing := isolateWrapperTitle(c, tc.key, b, metadata)
			if (routing != "") != tc.isolate {
				t.Fatal("wrong classification")
			}
			if tc.isolate {
				if routing == "title-parent" || routing == service.WrapperManagedAutoKey(1, "title-parent") {
					t.Fatal("routing not isolated")
				}
				if autoWrapperConversationID(c, 1, true, false, metadata) != "" {
					t.Fatal("title would acquire main managed state")
				}
				if c.GetHeader("X-Wrapper-Session-Mode") != "stateless" || c.GetHeader("X-Wrapper-Ack-Turn-Id") != "" || c.GetHeader("X-Wrapper-Session-Id") != "" {
					t.Fatal("managed controls leaked")
				}
			} else if c.GetHeader("X-Wrapper-Ack-Turn-Id") != "main-receipt" {
				t.Fatal("unrelated traffic changed")
			}
		})
	}
}
