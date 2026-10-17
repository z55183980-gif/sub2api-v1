package handler

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

func TestWrapperManagedBeginError(t *testing.T) {
	for _, tc := range []struct {
		name     string
		err      error
		canceled bool
		status   int
		typ      string
	}{
		{"busy", service.ErrWrapperManagedBusy, false, 409, "session_busy"},
		{"store", errors.New("redis secret details"), false, 503, "session_state_error"},
		{"canceled", context.Canceled, true, 499, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRecorder()
			c, _ := gin.CreateTestContext(r)
			c.Request = httptest.NewRequest("POST", "/v1/messages", nil)
			if tc.canceled {
				ctx, cancel := context.WithCancel(c.Request.Context())
				cancel()
				c.Request = c.Request.WithContext(ctx)
			}
			h := &GatewayHandler{}
			h.handleWrapperManagedBeginError(c, tc.err, false)
			c.Writer.WriteHeaderNow()
			if r.Code != tc.status || (tc.typ != "" && !strings.Contains(r.Body.String(), tc.typ)) {
				t.Fatalf("%d %s", r.Code, r.Body.String())
			}
			if (r.Header().Get("Retry-After") == "1") != (tc.status == 409) {
				t.Fatal("wrong retry hint")
			}
			if strings.Contains(r.Body.String(), "secret details") {
				t.Fatal("leaked storage error")
			}
		})
	}
}
