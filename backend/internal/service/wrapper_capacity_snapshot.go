package service

import (
	"context"
	"encoding/json"
	"golang.org/x/sync/singleflight"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Separate physical CLI occupancy from the gateway's active HTTP count.
// Nil counts mean unavailable telemetry, never zero occupancy.
type WrapperCapacitySnapshot struct {
	ObservedAt     time.Time `json:"observed_at"`
	Valid          bool      `json:"valid"`
	Generating     *int      `json:"generating"`
	WaitingTools   *int      `json:"waiting_tools"`
	IdleRetained   *int      `json:"idle_retained"`
	Queued         *int      `json:"queued"`
	TotalProcesses *int      `json:"total_processes"`
	Limit          *int      `json:"limit"`
	Available      *int      `json:"available"`
	Draining       bool      `json:"draining"`
}

type wrapperCapacityCache struct {
	mu      sync.Mutex
	entries map[int64]*WrapperCapacitySnapshot
	flight  singleflight.Group
}

func (s *GatewayService) cachedWrapperCapacity(id int64) *WrapperCapacitySnapshot {
	s.wrapperCapacity.mu.Lock()
	defer s.wrapperCapacity.mu.Unlock()
	value := s.wrapperCapacity.entries[id]
	if value != nil && time.Since(value.ObservedAt) < 5*time.Second {
		copy := *value
		return &copy
	}
	return nil
}

func decodeWrapperCapacity(reader io.Reader) *WrapperCapacitySnapshot {
	result := &WrapperCapacitySnapshot{ObservedAt: time.Now().UTC()}
	var health struct {
		Draining  bool `json:"draining"`
		Available *int `json:"availableProcesses"`
		Processes struct {
			Total      *int `json:"total"`
			Generating *int `json:"generating"`
			Waiting    *int `json:"waitingForTools"`
			Idle       *int `json:"idleHot"`
			Limit      *int `json:"limit"`
		} `json:"cliProcesses"`
		Sessions struct {
			Queued *int `json:"queued"`
		} `json:"sessions"`
	}
	if json.NewDecoder(io.LimitReader(reader, 16<<10)).Decode(&health) != nil {
		return result
	}
	p := health.Processes
	if p.Total == nil || p.Generating == nil || p.Waiting == nil || p.Limit == nil {
		return result
	}
	idle := 0
	if p.Idle != nil {
		idle = *p.Idle
	}
	for _, value := range []*int{p.Total, p.Generating, p.Waiting, p.Limit, p.Idle, health.Available, health.Sessions.Queued} {
		if value != nil && (*value < 0 || *value > 1000000) {
			return result
		}
	}
	if *p.Generating+*p.Waiting+idle != *p.Total {
		return result
	}
	result.Valid = true
	result.Generating, result.WaitingTools, result.IdleRetained = p.Generating, p.Waiting, &idle
	result.TotalProcesses, result.Limit, result.Available = p.Total, p.Limit, health.Available
	result.Queued, result.Draining = health.Sessions.Queued, health.Draining
	return result
}

// Admin observation uses the same URL validation, proxy and credentials as inference.
func (s *GatewayService) WrapperCapacitySnapshot(ctx context.Context, account *Account) *WrapperCapacitySnapshot {
	if account == nil || account.GetExtraString("wrapper_protocol") != "anthropic-messages-cli-v1" {
		return nil
	}
	if s == nil {
		return &WrapperCapacitySnapshot{ObservedAt: time.Now().UTC()}
	}
	if cached := s.cachedWrapperCapacity(account.ID); cached != nil {
		return cached
	}
	result, _, _ := s.wrapperCapacity.flight.Do(strconv.FormatInt(account.ID, 10), func() (any, error) {
		if cached := s.cachedWrapperCapacity(account.ID); cached != nil {
			return cached, nil
		}
		value := s.probeWrapperCapacity(ctx, account)
		s.wrapperCapacity.mu.Lock()
		if s.wrapperCapacity.entries == nil {
			s.wrapperCapacity.entries = make(map[int64]*WrapperCapacitySnapshot)
		}
		if len(s.wrapperCapacity.entries) >= 2048 {
			for id, old := range s.wrapperCapacity.entries {
				if time.Since(old.ObservedAt) >= 5*time.Second {
					delete(s.wrapperCapacity.entries, id)
				}
			}
		}
		if len(s.wrapperCapacity.entries) < 2048 {
			s.wrapperCapacity.entries[account.ID] = value
		}
		s.wrapperCapacity.mu.Unlock()
		return value, nil
	})
	copy := *result.(*WrapperCapacitySnapshot)
	return &copy
}

func (s *GatewayService) probeWrapperCapacity(ctx context.Context, account *Account) *WrapperCapacitySnapshot {
	unknown := &WrapperCapacitySnapshot{ObservedAt: time.Now().UTC()}
	if s == nil || s.httpUpstream == nil {
		return unknown
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	token, kind, err := s.GetAccessToken(ctx, account)
	if err != nil || kind != "apikey" {
		return unknown
	}
	base, err := s.validateUpstreamBaseURL(account.GetBaseURL())
	if err != nil {
		return unknown
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(base, "/")+"/health", nil)
	if err != nil {
		return unknown
	}
	setAnthropicAPIKeyAuthHeader(req.Header, account, token, account.GetBaseURL())
	account.ApplyHeaderOverrides(req.Header)
	proxyURL := ""
	if account.ProxyID != nil && account.Proxy != nil {
		proxyURL = account.Proxy.URL()
	}
	resp, err := s.httpUpstream.DoWithTLS(req, proxyURL, account.ID, account.Concurrency, s.tlsFPProfileService.ResolveTLSProfile(account))
	if err != nil || resp == nil {
		return unknown
	}
	if resp.Body == nil {
		return unknown
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return unknown
	}
	return decodeWrapperCapacity(resp.Body)
}
