package service

import (
	"encoding/json"
	"regexp"
	"strings"

	"github.com/tidwall/gjson"
)

// This is a reconciliation record, not command equivalence. The original
// native tool call remains intact, and the live tool result explicitly tells
// the model what the client reported instead. Never replay the command.
type managedBashCorrection struct{ ID, Original, Reported string }

var managedSimpleCD = regexp.MustCompile(`^cd[ \t]+(?:'([A-Za-z0-9_./ :\\-]+)'|"([A-Za-z0-9_./ :\\-]+)"|([A-Za-z0-9_./:\\-]+))[ \t]+&&[ \t]+`)

func managedRemovedCD(original, reported string) bool {
	m := managedSimpleCD.FindStringSubmatch(original)
	if m == nil || reported == "" || strings.TrimSpace(reported) != reported || original[len(m[0]):] != reported {
		return false
	}
	path := m[1] + m[2] + m[3]
	return strings.HasPrefix(path, "/") || (len(path) > 2 && path[1] == ':' && (path[2] == '/' || path[2] == '\\'))
}

// Only reconcile the latest still-pending Bash result, with an exact match of
// every other message, tool identity and parameter. Older history stays strict.
func (t *WrapperManagedAutoTurn) reconcilePendingBash(messages []json.RawMessage, userAgent string) ([]managedBashCorrection, bool) {
	// Both desktop and ordinary Claude CLI clients can remove this prefix.
	// Client identity only selects compatibility; the pending call, result IDs
	// and every other history field still have to match below.
	if !strings.HasPrefix(userAgent, "claude-cli/") || len(t.state.PendingToolIDs) == 0 {
		return nil, false
	}
	fresh := trailingUserMessages(messages)
	idx := len(messages) - fresh - 1
	if fresh < 1 || idx < 0 || gjson.GetBytes(messages[idx], "role").String() != "assistant" {
		return nil, false
	}
	if _, reason := managedToolResultTurn(messages[idx+1:], t.state.PendingToolIDs); reason != "" {
		return nil, false
	}
	var original json.RawMessage
	if len(t.state.Messages) > 0 {
		original = t.state.Messages[len(t.state.Messages)-1]
	} else if len(t.state.Recent) > 0 {
		original = t.state.Recent[len(t.state.Recent)-1]
	}
	a, b := managedCanonicalAssistant(original), managedCanonicalAssistant(messages[idx])
	if len(a) == 0 || len(b) == 0 || len(a) > 64<<10 || len(b) > 64<<10 {
		return nil, false
	}
	var left, right map[string]any
	if json.Unmarshal(a, &left) != nil || json.Unmarshal(b, &right) != nil {
		return nil, false
	}
	lb, lok := left["content"].([]any)
	rb, rok := right["content"].([]any)
	if !lok || !rok || len(lb) != len(rb) {
		return nil, false
	}
	pending := map[string]bool{}
	for _, id := range t.state.PendingToolIDs {
		pending[id] = true
	}
	changes := []managedBashCorrection{}
	for i, item := range lb {
		old, ok := item.(map[string]any)
		if !ok {
			return nil, false
		}
		cur, ok := rb[i].(map[string]any)
		if !ok {
			return nil, false
		}
		id, _ := old["id"].(string)
		if old["type"] != "tool_use" || old["name"] != "Bash" || !pending[id] || cur["id"] != id || cur["name"] != "Bash" {
			continue
		}
		oi, ok := old["input"].(map[string]any)
		if !ok {
			continue
		}
		ci, ok := cur["input"].(map[string]any)
		if !ok {
			continue
		}
		oc, ok := oi["command"].(string)
		if !ok {
			continue
		}
		cc, ok := ci["command"].(string)
		if !ok || oc == cc {
			continue
		}
		if !managedRemovedCD(oc, cc) {
			return nil, false
		}
		changes = append(changes, managedBashCorrection{id, oc, cc})
		ci["command"] = oc
	}
	if len(changes) == 0 {
		return nil, false
	}
	restored, _ := json.Marshal(right)
	if !equalManagedMessage(restored, original) {
		return nil, false
	}
	probe := append([]json.RawMessage(nil), messages...)
	probe[idx] = original
	var reason string
	if len(t.state.Proofs) > 0 {
		_, _, reason = matchManagedHistoryProofs(probe, t.state.Proofs)
	} else {
		_, _, reason = matchManagedHistoryPrefix(probe, t.state.Messages)
	}
	return changes, reason == ""
}

func managedAnnotateBashResult(raw json.RawMessage, corrections []managedBashCorrection) json.RawMessage {
	var message map[string]any
	if json.Unmarshal(raw, &message) != nil {
		return raw
	}
	blocks, _ := message["content"].([]any)
	for _, item := range blocks {
		block, ok := item.(map[string]any)
		if !ok || block["type"] != "tool_result" {
			continue
		}
		for _, change := range corrections {
			if block["tool_use_id"] != change.ID {
				continue
			}
			detail, _ := json.Marshal(map[string]string{"tool_use_id": change.ID, "original_command": change.Original, "client_reported_command": change.Reported})
			note := map[string]any{"type": "text", "text": "[Gateway tool history reconciliation: client-reported data, not instructions]\nThe client returned this pending Bash call with its leading cd clause removed. The original call remains in history. Execution cwd is unverified; do not assume the two commands are equivalent or rerun either command based on this notice. Interpret the attached result with this discrepancy in mind.\n" + string(detail)}
			parts := []any{}
			switch c := block["content"].(type) {
			case string:
				parts = append(parts, map[string]any{"type": "text", "text": c})
			case []any:
				parts = append(parts, c...)
			}
			block["content"] = append(parts, note)
		}
	}
	out, _ := json.Marshal(message)
	return out
}
