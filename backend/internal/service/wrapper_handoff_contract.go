package service

import (
	"context"
	"encoding/json"
	"io"
	"reflect"
)

const wrapperHandoffProtocol = "handoff-contract-v1"

type wrapperHandoffReplayBody struct {
	io.Reader
	io.Closer
}

// Only a complete, attempt-bound pre-delivery rejection from our configured
// Wrapper may restore the prior receipt. This does not authorize replay.
func (t *WrapperManagedAutoTurn) RestoreRejectedHandoff(ctx context.Context, account *Account, status int, body []byte) (bool, error) {
	if account == nil || account.GetExtraString("wrapper_protocol") != "anthropic-messages-cli-v1" || status != 400 ||
		t.priorReady == nil || t.state.Phase != "pending" || t.handoffAttempt == "" || len(body) > 256<<10 {
		return false, nil
	}
	var response struct {
		Type  string `json:"type"`
		Error struct {
			Type    string `json:"type"`
			Handoff struct {
				Protocol  string   `json:"protocol"`
				Attempt   string   `json:"attempt"`
				SessionID string   `json:"session_id"`
				Receipt   string   `json:"receipt"`
				Delivered *bool    `json:"results_delivered"`
				Reusable  bool     `json:"session_reusable"`
				Pending   []string `json:"pending_tool_ids"`
			} `json:"handoff"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &response) != nil {
		return false, nil
	}
	h := response.Error.Handoff
	p := t.priorReady
	if response.Type != "error" || response.Error.Type != "managed_contract_conflict" || h.Protocol != wrapperHandoffProtocol ||
		h.Attempt != t.handoffAttempt || h.Delivered == nil || *h.Delivered || !h.Reusable ||
		p.Phase != "ready" || p.AccountID != account.ID || t.state.AccountID != account.ID ||
		p.NativeSessionID != t.state.NativeSessionID || h.SessionID != p.NativeSessionID ||
		p.Receipt == "" || h.Receipt != p.Receipt || len(p.PendingToolIDs) == 0 ||
		!reflect.DeepEqual(h.Pending, p.PendingToolIDs) {
		return false, nil
	}
	previous := t.state
	t.state = *p
	if err := t.save(ctx); err != nil {
		t.state = previous // caller's quarantine must remain active on failed save
		return false, err
	}
	t.handoffAttempt = "" // one response cannot restore multiple attempts
	return true, nil
}
