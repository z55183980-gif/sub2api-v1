package service

import (
	"context"
	"encoding/json"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestWrapperRejectedHandoffRestoresOnlyExactAttempt(t *testing.T) {
	for _, change := range []string{"valid", "attempt", "receipt", "session", "pending", "delivered", "missing_delivered", "protocol", "type", "account", "truncated"} {
		t.Run(change, func(t *testing.T) {
			ctx := context.Background()
			store := newWrapperManagedTestStore()
			turn, err := beginWrapperManagedAutoTurn(ctx, store, WrapperManagedAutoKey(42, change))
			require.NoError(t, err)
			defer turn.Release()
			prior := wrapperManagedState{AccountID: 8, NativeSessionID: "sub2api-test", Phase: "ready", Receipt: "receipt", PendingToolIDs: []string{"toolu_one", "toolu_two"}}
			turn.priorReady = &prior
			turn.state = prior
			turn.state.Phase = "pending"
			turn.handoffAttempt = strings.Repeat("a", 32)
			require.NoError(t, turn.save(ctx))
			account := &Account{ID: 8, Extra: map[string]any{"wrapper_protocol": "anthropic-messages-cli-v1"}}
			h := map[string]any{"protocol": wrapperHandoffProtocol, "attempt": turn.handoffAttempt, "session_id": prior.NativeSessionID, "receipt": prior.Receipt, "results_delivered": false, "session_reusable": true, "pending_tool_ids": prior.PendingToolIDs}
			typ := "managed_contract_conflict"
			switch change {
			case "attempt":
				h["attempt"] = "stale"
			case "receipt":
				h["receipt"] = "stale"
			case "session":
				h["session_id"] = "other"
			case "pending":
				h["pending_tool_ids"] = []string{"toolu_one"}
			case "delivered":
				h["results_delivered"] = true
			case "missing_delivered":
				delete(h, "results_delivered")
			case "protocol":
				h["protocol"] = "unknown"
			case "type":
				typ = "invalid_request_error"
			case "account":
				account.ID = 9
			}
			body, _ := json.Marshal(map[string]any{"type": "error", "error": map[string]any{"type": typ, "handoff": h}})
			if change == "truncated" {
				body = body[:len(body)-1]
			}
			restored, err := turn.RestoreRejectedHandoff(ctx, account, 400, body)
			require.NoError(t, err)
			require.Equal(t, change == "valid", restored)
			if restored {
				require.Equal(t, prior, turn.state)
				restored, err = turn.RestoreRejectedHandoff(ctx, account, 400, body)
				require.NoError(t, err)
				require.False(t, restored)
			} else {
				require.Equal(t, "pending", turn.state.Phase)
			}
		})
	}
}
