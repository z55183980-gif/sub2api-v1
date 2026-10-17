package service

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"
)

// Waiting tools retain their CLI slot after HTTP ends; existing handoffs
// bypass new-process admission because they already own their process.
func (s *GatewayService) wrapperProcessUnavailable(ctx context.Context, account *Account, turn *WrapperManagedAutoTurn, token, proxyURL string) bool {
	if turn == nil || (turn.wasReady && len(turn.state.PendingToolIDs) > 0) {
		return false
	}
	base, err := s.validateUpstreamBaseURL(account.GetBaseURL())
	if err != nil {
		return false
	}
	probe, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(probe, http.MethodGet, strings.TrimRight(base, "/")+"/health", nil)
	if err != nil {
		return false
	}
	setAnthropicAPIKeyAuthHeader(req.Header, account, token, account.GetBaseURL())
	account.ApplyHeaderOverrides(req.Header)
	resp, err := s.httpUpstream.DoWithTLS(req, proxyURL, account.ID, account.Concurrency, s.tlsFPProfileService.ResolveTLSProfile(account))
	if err != nil || resp == nil {
		return false
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return false
	}
	var h struct {
		Draining  bool `json:"draining"`
		Processes struct {
			Total int `json:"total"`
			Limit int `json:"limit"`
		} `json:"cliProcesses"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 16<<10)).Decode(&h) != nil {
		return false
	}
	return h.Draining || (h.Processes.Limit > 0 && h.Processes.Total >= h.Processes.Limit)
}
