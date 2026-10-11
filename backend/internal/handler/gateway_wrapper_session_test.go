package handler

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

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
