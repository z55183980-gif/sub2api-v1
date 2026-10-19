package handler

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

func TestAutoWrapperRequestClassesDoNotShareMainHistory(t *testing.T) {
	t.Setenv(service.AutoWrapperManagedEnv, "1")
	t.Setenv(service.AutoWrapperManagedKeyIDsEnv, "1")
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
	metadata := `{"device_id":"test-device","session_id":"parent"}`
	for _, kind := range []string{"auxiliary", "compaction", "workflow", "unknown", "subagent"} {
		c.Request.Header.Set("X-Claude-Code-Request-Class", kind)
		if id := autoWrapperConversationID(c, 1, true, false, metadata); id != "" {
			t.Fatalf("%s inherited %s", kind, id)
		}
	}
	c.Request.Header.Set("X-Claude-Code-Request-Class", "main")
	if autoWrapperConversationID(c, 1, true, false, metadata) != "parent" {
		t.Fatal("main lost routing")
	}
}

func TestAutoWrapperConversationIDFromRealCLIHeaders(t *testing.T) {
	t.Setenv(service.AutoWrapperManagedEnv, "1")
	t.Setenv(service.AutoWrapperManagedKeyIDsEnv, "1")
	metadata := `{"device_id":"same-device","account_uuid":"","session_id":"f804bbdc-8197-469d-8c4d-542cde48203d"}`
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
	main := autoWrapperConversationID(c, 1, true, false, metadata)
	c.Request.Header.Set("X-Claude-Code-Agent-Id", "ae46f70080fa89993")
	child := autoWrapperConversationID(c, 1, true, false, metadata)
	if main != "f804bbdc-8197-469d-8c4d-542cde48203d" || child == main || child == "" {
		t.Fatal("CLI parent and child identity not separated")
	}
	if child != service.WrapperManagedConversationID(main, "ae46f70080fa89993") {
		t.Fatal("routing identity differs from state identity")
	}
	if autoWrapperConversationID(c, 1, false, false, metadata) != child {
		t.Fatal("nonstream identity differs")
	}
	t.Setenv("SUB2API_WRAPPER_NONSTREAM_DISABLED", "1")
	if autoWrapperConversationID(c, 2, true, false, metadata) != "" || autoWrapperConversationID(c, 1, false, false, metadata) != "" || autoWrapperConversationID(c, 1, true, true, metadata) != "" {
		t.Fatal("isolation changed traffic outside auto-managed scope")
	}
	for _, mode := range []string{"stateless", "managed"} {
		c.Request.Header.Set("X-Wrapper-Session-Mode", mode)
		if autoWrapperConversationID(c, 1, true, false, metadata) != "" {
			t.Fatal("explicit mode was overridden")
		}
	}
}

func TestManagedWrapperSessionID(t *testing.T) {
	gin.SetMode(gin.TestMode)
	tests := []struct {
		name, mode, wrapperID, claudeID, want string
		managed, invalid                      bool
	}{
		{name: "stateless", wrapperID: "abc", managed: false},
		{name: "invalid mode", mode: "unknown", invalid: true},
		{name: "wrapper header", mode: "managed", wrapperID: "abc", want: "abc", managed: true},
		{name: "claude header", mode: "managed", claudeID: "abc", want: "abc", managed: true},
		{name: "matching headers", mode: "managed", wrapperID: "abc", claudeID: "abc", want: "abc", managed: true},
		{name: "conflicting headers", mode: "managed", wrapperID: "abc", claudeID: "def", invalid: true},
		{name: "missing ID", mode: "managed", invalid: true},
		{name: "invalid ID", mode: "managed", wrapperID: "bad id", invalid: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)
			c.Request.Header.Set("X-Wrapper-Session-Mode", tc.mode)
			c.Request.Header.Set("X-Wrapper-Session-Id", tc.wrapperID)
			c.Request.Header.Set("X-Claude-Code-Session-Id", tc.claudeID)
			id, managed, err := managedWrapperSessionID(c)
			if (err != nil) != tc.invalid || id != tc.want || managed != tc.managed {
				t.Fatalf("got id=%q managed=%v err=%v", id, managed, err)
			}
			if tc.managed && (c.GetHeader("X-Wrapper-Session-Id") != tc.want || c.GetHeader("X-Claude-Code-Session-Id") != tc.want) {
				t.Fatal("managed session headers were not aligned")
			}
		})
	}
}
