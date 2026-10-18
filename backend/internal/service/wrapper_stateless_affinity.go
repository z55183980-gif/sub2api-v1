package service

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// Account affinity only: never resume or merge native conversations. Each full
// history has its own entry, so a failover on one branch cannot rebind siblings.
const statelessAffinityLimit = 8192
const statelessAffinityTTL = time.Hour

type statelessAffinityEntry struct {
	account int64
	expires time.Time
}

type statelessAffinityStore struct {
	mu      sync.Mutex
	entries map[string]statelessAffinityEntry
}

type WrapperStatelessAffinity struct {
	Key             string   `json:"key"`
	AccountID       int64    `json:"account_id"`
	MatchedMessages int      `json:"matched_messages"`
	MessageCount    int      `json:"message_count"`
	SystemHash      string   `json:"system_hash"`
	ToolsHash       string   `json:"tools_hash"`
	Source          string   `json:"source"`
	PrefixHashes    []string `json:"prefix_hashes"`
}

func affinityHash(value []byte) string {
	h := sha256.Sum256(value)
	return hex.EncodeToString(h[:])
}

func canonicalAffinity(value json.RawMessage) []byte {
	if len(value) == 0 {
		return []byte("null")
	}
	var v any
	d := json.NewDecoder(bytes.NewReader(value))
	d.UseNumber() // Preserve integer precision in historical tool arguments.
	if d.Decode(&v) != nil {
		return value
	}
	b, err := json.Marshal(v)
	if err != nil {
		return value
	}
	return b
}

// Cache hints on client-owned tools are not forwarded by the MCP bridge.
// Preserve native/typed tools and every other field; hint-free keys stay v1.
func wrapperAffinityTools(raw json.RawMessage) json.RawMessage {
	var tools []map[string]json.RawMessage
	if json.Unmarshal(raw, &tools) != nil {
		return raw
	}
	changed := false
	for _, tool := range tools {
		var name string
		_ = json.Unmarshal(tool["name"], &name)
		if name == "" || name == "WebSearch" || len(tool["type"]) != 0 || len(tool["input_schema"]) == 0 {
			continue
		}
		if _, ok := tool["cache_control"]; ok {
			delete(tool, "cache_control")
			changed = true
		}
	}
	if !changed {
		return raw
	}
	normalized, err := json.Marshal(tools)
	if err != nil {
		return raw
	}
	return normalized
}

// ResolveWrapperStatelessAffinity accepts only requests with no reliable session
// ID. Canonical JSON sorts object keys but preserves text, array order, tool IDs
// and all history content. A modified/compacted history gets only the longest
// genuinely identical prefix; an unrelated first message gets no affinity.
func (s *GatewayService) ResolveWrapperStatelessAffinity(apiKeyID int64, groupID *int64, body []byte) *WrapperStatelessAffinity {
	var b struct {
		Model        string            `json:"model"`
		System       json.RawMessage   `json:"system"`
		Tools        json.RawMessage   `json:"tools"`
		ToolChoice   json.RawMessage   `json:"tool_choice"`
		Thinking     json.RawMessage   `json:"thinking"`
		OutputConfig json.RawMessage   `json:"output_config"`
		Messages     []json.RawMessage `json:"messages"`
	}
	if json.Unmarshal(body, &b) != nil || len(b.Messages) == 0 || len(b.Messages) > 2048 {
		return nil
	}
	b.Tools = wrapperAffinityTools(b.Tools)
	ns := fmt.Sprintf("%d:%d:%s:", apiKeyID, derefGroupID(groupID), b.Model)
	for _, v := range []json.RawMessage{b.System, b.Tools, b.ToolChoice, b.Thinking, b.OutputConfig} {
		ns += affinityHash(canonicalAffinity(v)) + ":"
	}
	prev := affinityHash([]byte(ns))
	a := &WrapperStatelessAffinity{MessageCount: len(b.Messages), SystemHash: affinityHash(canonicalAffinity(b.System)), ToolsHash: affinityHash(canonicalAffinity(b.Tools)), Source: "new_history"}
	for _, m := range b.Messages {
		prev = affinityHash(append([]byte(prev+":"), canonicalAffinity(m)...))
		a.PrefixHashes = append(a.PrefixHashes, "wrapper-prefix-v1:"+prev)
	}
	a.Key = a.PrefixHashes[len(a.PrefixHashes)-1]
	now := time.Now()
	s.statelessAffinity.mu.Lock()
	defer s.statelessAffinity.mu.Unlock()
	for i := len(a.PrefixHashes) - 1; i >= 0; i-- {
		entry, ok := s.statelessAffinity.entries[a.PrefixHashes[i]]
		if !ok {
			continue
		}
		if !entry.expires.After(now) {
			delete(s.statelessAffinity.entries, a.PrefixHashes[i])
			continue
		}
		a.AccountID = entry.account
		a.MatchedMessages = i + 1
		a.Source = "history_prefix"
		if i == len(a.PrefixHashes)-1 {
			a.Source = "exact_history"
		}
		break
	}
	return a
}

// Remember only an account which actually returned model usage. In particular,
// a failed selection/429 must not poison the history index. Partial metered
// responses are included because clients may close after receiving message_stop.
func (s *GatewayService) RememberWrapperStatelessAffinity(a *WrapperStatelessAffinity, accountID int64) {
	if a == nil || a.Key == "" || accountID <= 0 {
		return
	}
	now := time.Now()
	s.statelessAffinity.mu.Lock()
	defer s.statelessAffinity.mu.Unlock()
	if s.statelessAffinity.entries == nil {
		s.statelessAffinity.entries = make(map[string]statelessAffinityEntry)
	}
	if len(s.statelessAffinity.entries) >= statelessAffinityLimit {
		oldestKey := ""
		oldest := now.Add(2 * statelessAffinityTTL)
		for k, v := range s.statelessAffinity.entries {
			if !v.expires.After(now) {
				delete(s.statelessAffinity.entries, k)
				continue
			}
			if v.expires.Before(oldest) {
				oldestKey = k
				oldest = v.expires
			}
		}
		if len(s.statelessAffinity.entries) >= statelessAffinityLimit {
			delete(s.statelessAffinity.entries, oldestKey)
		}
	}
	s.statelessAffinity.entries[a.Key] = statelessAffinityEntry{account: accountID, expires: now.Add(statelessAffinityTTL)}
}
