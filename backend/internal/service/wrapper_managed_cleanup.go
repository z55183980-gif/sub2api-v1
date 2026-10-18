package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

var errWrapperCleanupBusy = errors.New("previous wrapper session is still active")
var errWrapperCleanupUnavailable = errors.New("previous wrapper cleanup transport unavailable")

// Only server-generated session IDs on registered Wrapper accounts may be deleted.
// Use the same account credential, routing and transport as inference requests.
func (s *GatewayService) deleteAbandonedWrapperSession(ctx context.Context, accountID int64, id string) error {
	if !strings.HasPrefix(id, "sub2api-") || len(id) > 128 || strings.ContainsAny(id, "/\\?#%") {
		return fmt.Errorf("invalid automatic wrapper session ID")
	}
	account, err := s.accountRepo.GetByID(ctx, accountID)
	if err != nil {
		return fmt.Errorf("load previous wrapper account: %w", err)
	}
	if account == nil || account.GetExtraString("wrapper_protocol") != "anthropic-messages-cli-v1" {
		return fmt.Errorf("previous account is not a registered wrapper")
	}
	token, tokenType, err := s.GetAccessToken(ctx, account)
	if err != nil {
		return err
	}
	if tokenType != "apikey" {
		return fmt.Errorf("wrapper cleanup requires an API key account")
	}
	base, err := s.validateUpstreamBaseURL(account.GetBaseURL())
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, strings.TrimRight(base, "/")+"/v1/wrapper/sessions/"+url.PathEscape(id), nil)
	if err != nil {
		return err
	}
	setAnthropicAPIKeyAuthHeader(req.Header, account, token, account.GetBaseURL())
	account.ApplyHeaderOverrides(req.Header)
	proxyURL := ""
	if account.ProxyID != nil && account.Proxy != nil {
		proxyURL = account.Proxy.URL()
	}
	resp, err := s.httpUpstream.DoWithTLS(req, proxyURL, account.ID, account.Concurrency, s.tlsFPProfileService.ResolveTLSProfile(account))
	if err != nil {
		return fmt.Errorf("%w: %w", errWrapperCleanupUnavailable, err)
	}
	if resp == nil {
		return fmt.Errorf("empty wrapper cleanup response")
	}
	if resp.Body != nil {
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	}
	if resp.StatusCode == http.StatusConflict {
		return errWrapperCleanupBusy
	}
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("wrapper cleanup returned HTTP %d", resp.StatusCode)
	}
	return nil
}
