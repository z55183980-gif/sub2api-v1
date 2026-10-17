package handler

import (
	"errors"
	"net/http"

	"github.com/Wei-Shaw/sub2api/internal/service"
	"github.com/gin-gonic/gin"
)

func (h *GatewayHandler) handleWrapperManagedBeginError(c *gin.Context, err error, streamStarted bool) {
	if failoverClientGone(c) {
		return
	}
	if errors.Is(err, service.ErrWrapperManagedBusy) {
		c.Header("Retry-After", "1")
		h.handleStreamingAwareError(c, http.StatusConflict, "session_busy", err.Error(), streamStarted)
		return
	}
	h.handleStreamingAwareError(c, http.StatusServiceUnavailable, "session_state_error", "Managed session state is temporarily unavailable", streamStarted)
}
