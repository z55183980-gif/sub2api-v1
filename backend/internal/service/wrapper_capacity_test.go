package service

import (
	"context"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestWrapperProcessCapacityKeepsExistingToolContinuationAdmitted(t *testing.T) {
	account := newAnthropicAPIKeyAccountForTest()
	upstream := &anthropicHTTPUpstreamRecorder{resp: &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"cliProcesses":{"total":5,"limit":5}}`))}}
	svc := &GatewayService{httpUpstream: upstream, cfg: &config.Config{}}
	turn := &WrapperManagedAutoTurn{}
	if !svc.wrapperProcessUnavailable(context.Background(), account, turn, "test", "") {
		t.Fatal("full process capacity admitted")
	}
	upstream.lastReq = nil
	turn.wasReady = true
	turn.state.PendingToolIDs = []string{"toolu_live"}
	if svc.wrapperProcessUnavailable(context.Background(), account, turn, "test", "") {
		t.Fatal("existing handoff blocked")
	}
	if upstream.lastReq != nil {
		t.Fatal("existing handoff was probed")
	}
	turn.wasReady = false
	upstream.resp = &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"draining":true,"cliProcesses":{"total":0,"limit":5}}`))}
	if !svc.wrapperProcessUnavailable(context.Background(), account, turn, "test", "") {
		t.Fatal("draining node admitted")
	}
}

func TestWrapperCapacityUsesAvailableSlotsWithHotProcesses(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		blocked    bool
	}{
		{"five idle", `{"availableProcesses":5,"cliProcesses":{"total":5,"limit":5,"idleHot":5}}`, false},
		{"physical reserve reclaimable", `{"availableProcesses":1,"cliProcesses":{"total":6,"limit":5,"idleHot":1}}`, false},
		{"generation full", `{"availableProcesses":0,"cliProcesses":{"total":5,"limit":5}}`, true},
		{"drain wins", `{"draining":true,"availableProcesses":5}`, true},
		{"legacy full", `{"cliProcesses":{"total":5,"limit":5}}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			upstream := &anthropicHTTPUpstreamRecorder{resp: &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(tc.body))}}
			svc := &GatewayService{httpUpstream: upstream, cfg: &config.Config{}}
			if got := svc.wrapperProcessUnavailable(context.Background(), newAnthropicAPIKeyAccountForTest(), &WrapperManagedAutoTurn{}, "test", ""); got != tc.blocked {
				t.Fatalf("blocked=%v", got)
			}
		})
	}
}
