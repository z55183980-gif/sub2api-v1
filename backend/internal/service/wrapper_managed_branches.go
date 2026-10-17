package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// A parent CLI session is a namespace, not a single mutable conversation.
// Only metadata/proofs are indexed here; generation holds the branch lock.
type managedBranchEntry struct {
	ID      string `json:"id"`
	Seed    string `json:"seed,omitempty"`
	Created int64  `json:"created"`
}
type managedBranchIndex struct {
	Entries []managedBranchEntry `json:"entries"`
}

var managedBranchRecoveryMessage = json.RawMessage(`{"role":"user","content":"Continue from the completed tool results in the preceding conversation history. Those tool calls have already executed; do not repeat them merely to reconstruct this session. Respond to the outstanding task using the recorded results."}`)

// Closed client tool history can bootstrap an independent native conversation.
// The added user turn is transport-only; it is never added to the client ledger.
func managedBranchRecoveryHistory(messages []json.RawMessage) ([]json.RawMessage, bool) {
	if len(messages) == 0 || !hasManagedToolResult(messages[len(messages)-1]) {
		return nil, false
	}
	full := append(append([]json.RawMessage(nil), messages...), managedBranchRecoveryMessage)
	return full, validManagedBootstrapHistory(full)
}

// ResolveWrapperManagedBranch is for legacy desktop requests without Agent-ID.
// It never changes or deletes a native session. Prepare still validates the
// selected transcript under the branch lock before handing results to the CLI.
func (s *GatewayService) ResolveWrapperManagedBranch(ctx context.Context, keyID int64, parent string, body []byte, beta, ua string) (string, string, error) {
	return resolveWrapperManagedBranch(ctx, s.WrapperManagedStore(), keyID, parent, body, beta, ua)
}

func resolveWrapperManagedBranch(ctx context.Context, store WrapperManagedStore, keyID int64, parent string, body []byte, beta, ua string) (string, string, error) {
	normalized, err := normalizedWrapperManagedBody(body, beta)
	if err != nil {
		return "", "", err
	}
	var req struct {
		Model    string            `json:"model"`
		System   json.RawMessage   `json:"system"`
		Messages []json.RawMessage `json:"messages"`
	}
	if err = json.Unmarshal(normalized, &req); err != nil {
		return "", "", err
	}
	// Retain every request field in the bootstrap digest (tools, thinking, etc).
	// Canonical JSON removes irrelevant object-key ordering, not history changes.
	var value any
	if err = json.Unmarshal(normalized, &value); err != nil {
		return "", "", err
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return "", "", err
	}
	sum := sha256.Sum256(canonical)
	seed := hex.EncodeToString(sum[:])
	indexKey := "branches-v1:" + WrapperManagedAutoKey(keyID, parent)
	// Hard operation deadline is shorter than the index lease. Never hold this
	// lock during account selection, inference, or tool execution.
	op, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var token string
	for {
		var acquired bool
		token, acquired, err = store.AcquireWrapperManagedLock(op, indexKey, 15*time.Second)
		if err != nil {
			return "", "", err
		}
		if acquired {
			break
		}
		select {
		case <-op.Done():
			return "", "", op.Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), time.Second)
		defer done()
		_ = store.ReleaseWrapperManagedLock(cleanup, indexKey, token)
	}()
	raw, err := store.GetWrapperManagedState(op, indexKey)
	if err != nil {
		return "", "", err
	}
	index := managedBranchIndex{}
	if len(raw) > 0 {
		if err = json.Unmarshal(raw, &index); err != nil {
			return "", "", fmt.Errorf("invalid managed branch index: %w", err)
		}
	}
	if len(index.Entries) == 0 {
		index.Entries = append(index.Entries, managedBranchEntry{ID: parent})
	}
	now := time.Now().Unix()
	kept := make([]managedBranchEntry, 0, len(index.Entries)+1)
	matches := []string{}
	seedMatch := ""
	completedSeed := false
	for _, entry := range index.Entries {
		data, e := store.GetWrapperManagedState(op, WrapperManagedAutoKey(keyID, entry.ID))
		if e != nil {
			return "", "", e
		}
		var state wrapperManagedState
		if len(data) > 0 {
			if e = json.Unmarshal(data, &state); e != nil {
				return "", "", e
			}
		}
		// A freshly registered branch may not have reached Prepare yet.
		if len(data) == 0 && now-entry.Created > 60 {
			continue
		}
		kept = append(kept, entry)
		if entry.Seed == seed {
			seedMatch = entry.ID
			completedSeed = state.Phase == "ready" && state.Receipt != ""
		}
		if state.Phase != "ready" || state.Model != req.Model || !equalManagedSystem(state.System, req.System) {
			continue
		}
		var reason string
		if len(state.Proofs) > 0 {
			_, _, reason = matchManagedHistoryProofs(req.Messages, state.Proofs)
		} else {
			_, _, reason = matchManagedHistoryPrefix(req.Messages, state.Messages)
		}
		if reason == "history_changed" {
			// Probe on a copy. Bash reconciliation remains exclusively in Prepare.
			probe := &WrapperManagedAutoTurn{state: state}
			if _, ok := probe.reconcilePendingBash(req.Messages, ua); ok {
				reason = ""
			}
		}
		if reason == "" && state.Receipt != "" {
			matches = append(matches, entry.ID)
		}
	}
	id, source := "", ""
	switch {
	case len(matches) == 1:
		id, source = matches[0], "history"
	case len(matches) > 1:
		// No arbitrary first-match choice between distinct native states.
		return "", "", &WrapperManagedPayloadError{Status: 409, Reason: "managed branch history is ambiguous"}
	case seedMatch != "":
		if completedSeed {
			return "", "", &WrapperManagedPayloadError{Status: 409, Reason: "managed branch request already completed; continue with its response history"}
		}
		id, source = seedMatch, "request_retry"
	default:
		_, recoverable := managedBranchRecoveryHistory(req.Messages)
		if !validManagedBootstrapHistory(req.Messages) && !recoverable {
			return "", "", &WrapperManagedPayloadError{Status: 400, Reason: "managed branch requires a complete valid history"}
		}
		if len(kept) >= 128 {
			return "", "", &WrapperManagedPayloadError{Status: 429, Reason: "managed branch limit reached"}
		}
		framed, _ := json.Marshal([]string{"desktop-branch-v1", parent, seed})
		digest := sha256.Sum256(framed)
		id, source = "claude-branch-"+hex.EncodeToString(digest[:]), "new_history"
		kept = append(kept, managedBranchEntry{ID: id, Seed: seed, Created: now})
	}
	// Remember the selected request before inference, including continuation
	// requests. A duplicate arriving while that branch is pending must wait on
	// its existing lock rather than allocate another branch.
	for i := range kept {
		if kept[i].ID == id {
			kept[i].Seed = seed
			kept[i].Created = now
		}
	}
	encoded, err := json.Marshal(managedBranchIndex{Entries: kept})
	if err != nil {
		return "", "", err
	}
	ok, err := store.SetWrapperManagedStateOwned(op, indexKey, token, encoded, wrapperManagedTTL+time.Minute)
	if err != nil {
		return "", "", err
	}
	if !ok {
		return "", "", fmt.Errorf("managed branch index lease lost")
	}
	return id, source, nil
}
