package handler

import (
	"io"
	"net/http"

	middleware2 "github.com/Wei-Shaw/sub2api/internal/server/middleware"
	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

func (h *GatewayHandler) WrapperExecutionControl(c *gin.Context) {
	key, ok := middleware2.GetAPIKeyFromContext(c)
	if !ok {
		h.errorResponse(c, 401, "authentication_error", "Invalid API key")
		return
	}
	var body []byte
	if c.Request.Method == http.MethodPost {
		var err error
		body, err = io.ReadAll(io.LimitReader(c.Request.Body, 8193))
		if err != nil || len(body) > 8192 {
			h.errorResponse(c, 400, "invalid_request_error", "Control body exceeds limit")
			return
		}
	}
	status, result, err := h.gatewayService.ProxyWrapperExecution(c.Request.Context(), key.ID, service.WrapperClientScope(c.GetHeader("X-Wrapper-Client-Scope")), c.Param("execution_id"), c.Request.Method, body)
	if err != nil {
		h.errorResponse(c, 503, "execution_control_unavailable", "Execution owner could not be reached; query the same execution")
		return
	}
	if status == 404 {
		h.errorResponse(c, 404, "not_found_error", "Execution not found")
		return
	}
	c.Header("Cache-Control", "no-store")
	c.Data(status, "application/json", result)
}
