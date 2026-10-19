package handler

import (
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"net/http/httptest"
	"os"
	"testing"
)

func TestSummaryIsolationPrecedesManagedState(t *testing.T) {
	t.Setenv(service.AutoWrapperManagedEnv, "1")
	t.Setenv(service.AutoWrapperManagedKeyIDsEnv, "1,2")
	for _, fixture := range []string{"summary", "custom_summary", "custom_continuation"} {
		t.Run(fixture, func(t *testing.T) {
			b, e := os.ReadFile("../service/testdata/wrapper_summary/" + fixture + ".json")
			if e != nil {
				t.Fatal(e)
			}
			metadata := `{"session_id":"parent","device_id":"test"}`
			for _, tc := range []struct {
				name, mode, agent, disabled string
				key                         int64
				want                        bool
			}{
				{"summary", "", "child", "", 1, true}, {"other agent", "", "other", "", 1, true}, {"other key", "", "child", "", 2, true},
				{"explicit managed", "managed", "child", "", 1, false}, {"explicit stateless", "stateless", "child", "", 1, false},
				{"missing agent", "", "", "", 1, false}, {"unlisted key", "", "child", "", 9, false}, {"rollback", "", "child", "0", 1, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Setenv("SUB2API_WRAPPER_SUMMARY_STATELESS", tc.disabled)
					c, _ := gin.CreateTestContext(httptest.NewRecorder())
					c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
					c.Request.Header.Set("User-Agent", "claude-cli/2.1.300")
					c.Request.Header.Set("X-Claude-Code-Agent-Id", tc.agent)
					c.Request.Header.Set("X-Wrapper-Session-Mode", tc.mode)
					c.Request.Header.Set("X-Wrapper-Ack-Turn-Id", "receipt")
					c.Request.Header.Set("X-Wrapper-Session-Id", "state")
					id, out, err := isolateWrapperSummary(c, tc.key, b, metadata)
					if err != nil {
						t.Fatal(err)
					}
					if (id != "") != tc.want {
						t.Fatal("classification")
					}
					if tc.want {
						if autoWrapperConversationID(c, tc.key, true, false, metadata) != "" {
							t.Fatal("summary obtains managed state")
						}
						if c.GetHeader("X-Wrapper-Session-Id") != "" || c.GetHeader("X-Wrapper-Ack-Turn-Id") != "" || gjson.GetBytes(out, "tool_choice.type").String() != "none" {
							t.Fatal("unsafe summary")
						}
						if id == service.WrapperManagedConversationID("parent", tc.agent) {
							t.Fatal("identity collision")
						}
					} else if c.GetHeader("X-Wrapper-Ack-Turn-Id") != "receipt" {
						t.Fatal("unrelated traffic changed")
					}
				})
			}
		})
	}
}

func TestSummaryTokenNoticeIsolationPrecedesManagedState(t *testing.T) {
	t.Setenv(service.AutoWrapperManagedEnv, "1")
	t.Setenv(service.AutoWrapperManagedKeyIDsEnv, "1")
	t.Setenv("SUB2API_WRAPPER_SUMMARY_STATELESS", "1")
	for _, fixture := range []string{"summary", "continuation", "custom_summary", "custom_continuation"} {
		t.Run(fixture, func(t *testing.T) {
			body, err := os.ReadFile("../service/testdata/wrapper_summary/" + fixture + ".json")
			if err != nil {
				t.Fatal(err)
			}
			body, err = sjson.SetBytes(body, "messages.-1", map[string]string{"role": "user", "content": "<system_instruction>\n<total_tokens>14419258 tokens left</total_tokens>\n</system_instruction>"})
			if err != nil {
				t.Fatal(err)
			}
			metadata := `{"session_id":"parent","device_id":"test"}`
			c, _ := gin.CreateTestContext(httptest.NewRecorder())
			c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
			c.Request.Header.Set("User-Agent", "claude-cli/2.1.300")
			c.Request.Header.Set("X-Claude-Code-Agent-Id", "child")
			for _, h := range []string{"X-Wrapper-Session-Id", "X-Wrapper-Ack-Turn-Id", "X-Wrapper-Auto-Managed", "X-Idempotency-Key"} {
				c.Request.Header.Set(h, "main-state")
			}
			id, out, err := isolateWrapperSummary(c, 1, body, metadata)
			if err != nil {
				t.Fatal(err)
			}
			if id == "" || c.GetHeader("X-Wrapper-Session-Mode") != "stateless" || autoWrapperConversationID(c, 1, true, false, metadata) != "" {
				t.Fatal("summary still competes for managed session")
			}
			for _, h := range []string{"X-Wrapper-Session-Id", "X-Wrapper-Ack-Turn-Id", "X-Wrapper-Auto-Managed", "X-Idempotency-Key"} {
				if c.GetHeader(h) != "" {
					t.Fatalf("main state retained: %s", h)
				}
			}
			if gjson.GetBytes(out, "tool_choice.type").String() != "none" {
				t.Fatal("tools enabled")
			}
			if gjson.GetBytes(out, "messages").Raw != gjson.GetBytes(body, "messages").Raw {
				t.Fatal("history changed")
			}
		})
	}
}
