package service

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// Opt-in local integration entry: actual managed Prepare and gateway Forward,
// isolated store/account, only the pre-established loopback Wrapper tunnel.
func TestWrapperManagedImagesLiveGateway(t *testing.T) {
	if os.Getenv("WRAPPER_IMAGE_LIVE") != "1" {
		t.Skip("local image integration disabled")
	}
	gin.SetMode(gin.TestMode)
	dir := os.Getenv("WRAPPER_IMAGE_EVIDENCE")
	if dir == "" {
		t.Fatal("evidence directory required")
	}
	os.MkdirAll(dir, 0700)
	cfg := &config.Config{}
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	svc := &GatewayService{cfg: cfg, httpUpstream: &wrapperProbeHTTPUpstream{}, rateLimitService: &RateLimitService{}}
	account := newAnthropicAPIKeyAccountForTest()
	account.ID = 6
	account.Credentials["base_url"] = "http://127.0.0.1:24699"
	account.Extra["wrapper_protocol"] = "anthropic-messages-cli-v1"
	store := newWrapperManagedTestStore()
	done := make(chan struct{})
	var once sync.Once
	var mu sync.Mutex
	sequence := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/stop", func(w http.ResponseWriter, r *http.Request) { once.Do(func() { close(done) }); w.WriteHeader(204) })
	mux.HandleFunc("/v1/messages", func(w http.ResponseWriter, r *http.Request) {
		data, err := io.ReadAll(io.LimitReader(r.Body, 21<<20))
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		id := r.Header.Get("X-Image-Probe-Session")
		if !strings.HasPrefix(id, "sub2api-probe-") {
			http.Error(w, "test session required", 400)
			return
		}
		mu.Lock()
		n := sequence
		sequence++
		mu.Unlock()
		turn, err := beginWrapperManagedAutoTurn(r.Context(), store, WrapperManagedAutoKey(42, id))
		if err != nil {
			http.Error(w, err.Error(), 409)
			return
		}
		defer turn.Release()
		recorder := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(recorder)
		c.Request = r.Clone(r.Context())
		wire, active, err := turn.Prepare(r.Context(), 6, data, c)
		reason, _, _ := turn.Decision()
		evidence := map[string]any{"request": n, "active": active, "reason": reason, "nativeSession": turn.state.NativeSessionID, "hasAck": c.GetHeader("X-Wrapper-Ack-Turn-Id") != "", "inputBytes": len(data), "forwardBytes": len(wire)}
		if err == nil && !active {
			err = fmt.Errorf("unexpected non-managed path: %s", reason)
		}
		if err == nil {
			os.WriteFile(filepath.Join(dir, fmt.Sprintf("wire-%d.json", n)), wire, 0600)
			var parsed *ParsedRequest
			parsed, err = ParseGatewayRequest(NewRequestBodyRef(wire), PlatformAnthropic)
			if err == nil {
				parsed.AutoWrapperManagedTurn = turn
				_, err = svc.Forward(r.Context(), c, account, parsed)
			}
		}
		if err != nil {
			evidence["error"] = err.Error()
			if recorder.Body.Len() == 0 {
				recorder.WriteHeader(500)
				recorder.WriteString(err.Error())
			}
		}
		evidence["status"] = recorder.Code
		evidence["ready"] = turn.IsReady()
		encoded, _ := json.MarshalIndent(evidence, "", "  ")
		os.WriteFile(filepath.Join(dir, fmt.Sprintf("decision-%d.json", n)), encoded, 0600)
		os.WriteFile(filepath.Join(dir, fmt.Sprintf("response-%d.txt", n)), recorder.Body.Bytes(), 0600)
		for k, vs := range recorder.Header() {
			for _, v := range vs {
				w.Header().Add(k, v)
			}
		}
		w.WriteHeader(recorder.Code)
		w.Write(recorder.Body.Bytes())
	})
	listener, err := net.Listen("tcp", "127.0.0.1:24701")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: mux}
	go server.Serve(listener)
	t.Log("image gateway ready at 127.0.0.1:24701")
	select {
	case <-done:
	case <-time.After(6 * time.Minute):
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Shutdown(shutdown)
	for _, raw := range store.states {
		var state wrapperManagedState
		json.Unmarshal(raw, &state)
		if state.NativeSessionID != "" {
			req, _ := http.NewRequest("DELETE", "http://127.0.0.1:24699/v1/wrapper/sessions/"+state.NativeSessionID, nil)
			req.Header.Set("Authorization", "Bearer upstream-anthropic-key")
			res, e := http.DefaultClient.Do(req)
			if e == nil {
				res.Body.Close()
				if res.StatusCode != 204 {
					t.Errorf("cleanup returned %d", res.StatusCode)
				}
			} else {
				t.Errorf("cleanup failed: %v", e)
			}
		}
	}
}
