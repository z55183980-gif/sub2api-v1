package handler

import (
	"bytes"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

func TestRecapIsolationBeforeManagedState(t *testing.T) {
	t.Setenv(service.AutoWrapperManagedEnv, "1")
	t.Setenv(service.AutoWrapperManagedKeyIDsEnv, "1,2")
	body, err := os.ReadFile("../service/testdata/wrapper_recap/recap.json")
	if err != nil {
		t.Fatal(err)
	}
	metadata := `{"device_id":"test-device","session_id":"recap-parent"}`
	controls := []string{"X-Claude-Code-Session-Id", "X-Wrapper-Session-Id", "X-Wrapper-Ack-Turn-Id", "X-Wrapper-Auto-Managed", "X-Idempotency-Key"}
	for _, tc := range []struct {
		name, mode, class, agent, disabled, metadata, ua string
		key                                              int64
		want                                             bool
	}{
		{name: "missing class", key: 1, want: true},
		{name: "named recap", key: 1, class: "session_recap", want: true},
		{name: "agent recap", key: 1, agent: "child", want: true},
		{name: "explicit main", key: 1, class: "main"},
		{name: "explicit subagent", key: 1, class: "subagent", agent: "child"},
		{name: "other auxiliary", key: 1, class: "agent_summary"},
		{name: "explicit managed", key: 1, mode: "managed"},
		{name: "explicit stateless", key: 1, mode: "stateless"},
		{name: "other key", key: 9},
		{name: "rollback", key: 1, disabled: "0"},
		{name: "missing session", key: 1, metadata: `{}`},
		{name: "other client", key: 1, ua: "other/1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SUB2API_WRAPPER_RECAP_STATELESS", tc.disabled)
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest("POST", "/v1/messages", bytes.NewReader(body))
			c.Request.Header.Set("User-Agent", "claude-cli/2.1.302")
			if tc.ua != "" {
				c.Request.Header.Set("User-Agent", tc.ua)
			}
			c.Request.Header.Set("X-Wrapper-Session-Mode", tc.mode)
			c.Request.Header.Set("X-Claude-Code-Request-Class", tc.class)
			c.Request.Header.Set("X-Claude-Code-Agent-Id", tc.agent)
			for _, h := range controls {
				c.Request.Header.Set(h, "parent-control")
			}
			before := c.Request.Header.Clone()
			payload := append([]byte(nil), body...)
			uid := metadata
			if tc.metadata != "" {
				uid = tc.metadata
			}
			routing := isolateWrapperRecap(c, tc.key, body, uid)
			if (routing != "") != tc.want {
				t.Fatalf("unexpected routing %q", routing)
			}
			if !bytes.Equal(body, payload) {
				t.Fatal("payload changed")
			}
			if !tc.want {
				if !reflect.DeepEqual(before, c.Request.Header) {
					t.Fatal("unrelated request changed")
				}
				return
			}
			if c.GetHeader("X-Wrapper-Session-Mode") != "stateless" {
				t.Fatal("not stateless")
			}
			for _, h := range controls {
				if c.GetHeader(h) != "" {
					t.Fatalf("parent control leaked: %s", h)
				}
			}
			for _, streaming := range []bool{false, true} {
				if autoWrapperConversationID(c, tc.key, streaming, false, uid) != "" {
					t.Fatal("recap can acquire parent state")
				}
			}
			if id, managed, err := managedWrapperSessionID(c); err != nil || managed || id != "" {
				t.Fatal("recap can acquire explicit state")
			}
			// Retries remain isolated after an error/cancel, without recovering any
			// parent controls. Neither JSON replay nor managed cleanup gets a lease.
			if wrapperJSONReplayEnabled(c, "", false) {
				t.Fatal("recap acquired managed replay")
			}
		})
	}
	seen := map[string]bool{}
	for _, identity := range []struct {
		key          int64
		agent, scope string
	}{{1, "", ""}, {2, "", ""}, {1, "child", ""}, {1, "", "desktop"}} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
		c.Request.Header.Set("User-Agent", "claude-cli/2.1.302")
		c.Request.Header.Set("X-Claude-Code-Agent-Id", identity.agent)
		c.Request.Header.Set("X-Wrapper-Client-Scope", identity.scope)
		id := isolateWrapperRecap(c, identity.key, body, metadata)
		if id == "" || seen[id] {
			t.Fatalf("routing collision: %q", id)
		}
		seen[id] = true
	}
}
