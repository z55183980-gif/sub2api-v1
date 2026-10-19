package service

import (
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/tidwall/gjson"
)

type lifecycleLiveAccountRepo struct {
	AccountRepository
	account *Account
}

func (r *lifecycleLiveAccountRepo) GetByID(context.Context, int64) (*Account, error) {
	return r.account, nil
}

// Opt-in integration harness: real Prepare/Forward/control routes and native
// Wrapper over a pre-established loopback tunnel; no production data or keys.
func TestWrapperControlLiveGateway(t *testing.T) {
	if os.Getenv("WRAPPER_CONTROL_LIVE") != "1" {
		t.Skip("isolated lifecycle integration disabled")
	}
	t.Setenv("SUB2API_WRAPPER_EXECUTION_CONTROL", "1")
	dir := os.Getenv("WRAPPER_CONTROL_EVIDENCE")
	if dir == "" {
		t.Fatal("evidence directory required")
	}
	upstream := os.Getenv("WRAPPER_CONTROL_UPSTREAM")
	if !strings.HasPrefix(upstream, "http://127.0.0.1:") {
		t.Fatal("loopback upstream required")
	}
	cfg := &config.Config{}
	cfg.Security.URLAllowlist.AllowInsecureHTTP = true
	account := newAnthropicAPIKeyAccountForTest()
	account.ID = 6
	account.Concurrency = 2
	account.Credentials["base_url"] = upstream
	account.Credentials["api_key"] = "synthetic-control-key"
	account.Extra["wrapper_protocol"] = "anthropic-messages-cli-v1"
	account.Extra["wrapper_execution_control"] = "v1"
	store := &executionRouteStore{wrapperManagedTestStore: newWrapperManagedTestStore()}
	svc := &GatewayService{cfg: cfg, cache: store, accountRepo: &lifecycleLiveAccountRepo{account: account}, httpUpstream: &wrapperProbeHTTPUpstream{}, rateLimitService: &RateLimitService{accountRepo: &autoManagedFailoverAccountRepo{}}}
	gin.SetMode(gin.TestMode)
	router := gin.New()
	done := make(chan struct{}, 1)
	router.Use(func(c *gin.Context) {
		if c.GetHeader("Authorization") != "Bearer synthetic-local" {
			c.AbortWithStatus(401)
		}
	})
	router.POST("/stop", func(c *gin.Context) {
		select {
		case done <- struct{}{}:
		default:
		}
		c.Status(204)
	})
	control := func(c *gin.Context) {
		data, _ := io.ReadAll(io.LimitReader(c.Request.Body, 8193))
		status, out, err := svc.ProxyWrapperExecution(c.Request.Context(), 42, WrapperClientScope(c.GetHeader("X-Wrapper-Client-Scope")), c.Param("id"), c.Request.Method, data)
		if err != nil {
			c.String(503, "%s", err)
			return
		}
		c.Data(status, "application/json", out)
	}
	router.GET("/v1/wrapper/executions/:id", control)
	router.POST("/v1/wrapper/executions/:id/control", control)
	router.POST("/v1/messages/count_tokens", func(c *gin.Context) { c.JSON(200, gin.H{"input_tokens": 100}) })
	router.POST("/v1/messages", func(c *gin.Context) {
		data, err := io.ReadAll(io.LimitReader(c.Request.Body, 21<<20))
		if err != nil {
			c.Status(400)
			return
		}
		c.Set("wrapper_execution_key_id", int64(42))
		metadata := gjson.GetBytes(data, "metadata.user_id").String()
		id := gjson.Get(metadata, "session_id").String()
		if scope := WrapperClientScope(c.GetHeader("X-Wrapper-Client-Scope")); scope != "" {
			id = "scoped-" + HashUsageRequestPayload([]byte(scope+"\x00"+id))
		}
		// Auxiliary calls deliberately have no managed main-conversation owner.
		var turn *WrapperManagedAutoTurn
		if c.GetHeader("X-Claude-Code-Request-Class") == "main" {
			turn, _, err = svc.BeginWrapperManagedAutoTurn(c.Request.Context(), WrapperManagedAutoKey(42, id))
			if err != nil {
				c.String(409, "%s", err)
				return
			}
			defer turn.Release()
			var active bool
			data, active, err = turn.Prepare(c.Request.Context(), account.ID, data, c)
			if err != nil || !active {
				c.String(500, "prepare active=%v err=%v", active, err)
				return
			}
		}
		parsed, err := ParseGatewayRequest(NewRequestBodyRef(data), PlatformAnthropic)
		if err == nil {
			parsed.AutoWrapperManagedTurn = turn
			_, err = svc.Forward(c.Request.Context(), c, account, parsed)
		}
		if err != nil {
			if turn != nil {
				turn.Quarantine()
			}
			t.Logf("forward ended: %v", err)
			if !c.Writer.Written() {
				c.String(502, "%s", err)
			}
		}
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: router}
	go server.Serve(listener)
	evidence, _ := json.Marshal(map[string]string{"url": "http://" + listener.Addr().String()})
	if err = os.WriteFile(filepath.Join(dir, "gateway.json"), evidence, 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Minute):
		t.Error("integration client timed out")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Shutdown(ctx)
}
