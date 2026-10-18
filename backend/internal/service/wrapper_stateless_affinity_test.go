package service

import (
	"encoding/json"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
	"time"
)

func affinityBody(messages []any, system string) []byte {
	b, _ := json.Marshal(map[string]any{"model": "claude-opus-5-5", "system": system, "messages": messages})
	return b
}
func TestWrapperStatelessAffinityGrowthIsolationAndBranches(t *testing.T) {
	s := &GatewayService{}
	g := int64(1)
	first := []any{map[string]any{"role": "user", "content": "unique original task"}}
	a := s.ResolveWrapperStatelessAffinity(1, &g, affinityBody(first, "fixed"))
	require.Zero(t, a.AccountID)
	s.RememberWrapperStatelessAffinity(a, 6)
	growth := append(append([]any{}, first...), map[string]any{"role": "assistant", "content": "answer"}, map[string]any{"role": "user", "content": "continue"})
	b := s.ResolveWrapperStatelessAffinity(1, &g, affinityBody(growth, "fixed"))
	require.EqualValues(t, 6, b.AccountID)
	require.Equal(t, 1, b.MatchedMessages)
	require.NotEqual(t, a.Key, b.Key)
	// A branch may fail over without rebinding the parent or another branch.
	s.RememberWrapperStatelessAffinity(b, 4)
	require.EqualValues(t, 4, s.ResolveWrapperStatelessAffinity(1, &g, affinityBody(growth, "fixed")).AccountID)
	branch := append(append([]any{}, first...), map[string]any{"role": "user", "content": "different branch"})
	require.EqualValues(t, 6, s.ResolveWrapperStatelessAffinity(1, &g, affinityBody(branch, "fixed")).AccountID)
	require.Zero(t, s.ResolveWrapperStatelessAffinity(2, &g, affinityBody(growth, "fixed")).AccountID)
	g2 := int64(2)
	require.Zero(t, s.ResolveWrapperStatelessAffinity(1, &g2, affinityBody(growth, "fixed")).AccountID)
	require.Zero(t, s.ResolveWrapperStatelessAffinity(1, &g, affinityBody(growth, "changed instruction")).AccountID)
	require.Zero(t, s.ResolveWrapperStatelessAffinity(1, &g, affinityBody([]any{map[string]any{"role": "user", "content": "compacted history"}}, "fixed")).AccountID)
}

func TestWrapperStatelessAffinityCanonicalToolsExpiry(t *testing.T) {
	s := &GatewayService{}
	body := []byte(`{"model":"m","tools":[{"name":"read","input_schema":{"type":"object"}}],"messages":[{"role":"user","content":"task"},{"role":"assistant","content":[{"type":"tool_use","id":"call1","name":"read","input":{"b":2,"a":1}}]}]}`)
	a := s.ResolveWrapperStatelessAffinity(1, nil, body)
	s.RememberWrapperStatelessAffinity(a, 5)
	reordered := []byte(`{"messages":[{"content":"task","role":"user"},{"content":[{"input":{"a":1,"b":2},"name":"read","id":"call1","type":"tool_use"}],"role":"assistant"}],"tools":[{"input_schema":{"type":"object"},"name":"read"}],"model":"m"}`)
	require.EqualValues(t, 5, s.ResolveWrapperStatelessAffinity(1, nil, reordered).AccountID)
	var b map[string]any
	require.NoError(t, json.Unmarshal(body, &b))
	b["tools"] = []any{map[string]any{"name": "write"}}
	changed, _ := json.Marshal(b)
	require.Zero(t, s.ResolveWrapperStatelessAffinity(1, nil, changed).AccountID)
	s.statelessAffinity.entries[a.Key] = statelessAffinityEntry{account: 5, expires: time.Now().Add(-time.Second)}
	require.Zero(t, s.ResolveWrapperStatelessAffinity(1, nil, body).AccountID)
	require.Nil(t, s.ResolveWrapperStatelessAffinity(1, nil, []byte(`{"messages":[]}`)))
}

func TestWrapperStatelessAffinityBoundedAndUnserved(t *testing.T) {
	s := &GatewayService{}
	for i := 0; i < statelessAffinityLimit+10; i++ {
		s.RememberWrapperStatelessAffinity(&WrapperStatelessAffinity{Key: string(rune(i + 1))}, 1)
	}
	require.Len(t, s.statelessAffinity.entries, statelessAffinityLimit)
	a := s.ResolveWrapperStatelessAffinity(1, nil, affinityBody([]any{"hello"}, "fixed"))
	require.Zero(t, a.AccountID)
	s.RememberWrapperStatelessAffinity(a, 0)
	require.Zero(t, s.ResolveWrapperStatelessAffinity(1, nil, affinityBody([]any{"hello"}, "fixed")).AccountID)
}

func TestWrapperStatelessAffinityConcurrentAndNumericPrecision(t *testing.T) {
	s := &GatewayService{}
	a := []byte(`{"model":"m","messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"x","content":"ok"}]},{"role":"assistant","content":[{"type":"tool_use","id":"y","name":"read","input":{"id":9007199254740992}}]}]}`)
	b := []byte(`{"model":"m","messages":[{"role":"user","content":[{"type":"tool_result","tool_use_id":"x","content":"ok"}]},{"role":"assistant","content":[{"type":"tool_use","id":"y","name":"read","input":{"id":9007199254740993}}]}]}`)
	require.NotEqual(t, s.ResolveWrapperStatelessAffinity(1, nil, a).Key, s.ResolveWrapperStatelessAffinity(1, nil, b).Key)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				v := s.ResolveWrapperStatelessAffinity(1, nil, a)
				s.RememberWrapperStatelessAffinity(v, 6)
			}
		}()
	}
	wg.Wait()
	require.EqualValues(t, 6, s.ResolveWrapperStatelessAffinity(1, nil, a).AccountID)
	require.Zero(t, s.ResolveWrapperStatelessAffinity(1, nil, b).AccountID)
}

func TestWrapperStatelessAffinityClientCacheHint(t *testing.T) {
	s := &GatewayService{}
	a := []byte(`{"model":"m","tools":[{"name":"lookup","input_schema":{"type":"object"}}],"messages":[{"role":"user","content":"task"}]}`)
	b := []byte(`{"model":"m","tools":[{"name":"lookup","input_schema":{"type":"object"},"cache_control":{"type":"ephemeral"}}],"messages":[{"role":"user","content":"task"}]}`)
	first := s.ResolveWrapperStatelessAffinity(1, nil, a)
	s.RememberWrapperStatelessAffinity(first, 8)
	require.EqualValues(t, 8, s.ResolveWrapperStatelessAffinity(1, nil, b).AccountID)
	require.Equal(t, first.Key, s.ResolveWrapperStatelessAffinity(1, nil, b).Key)
	native := json.RawMessage(`[{"name":"WebSearch","cache_control":{"type":"ephemeral"}}]`)
	require.Equal(t, native, wrapperAffinityTools(native))
	typed := json.RawMessage(`[{"type":"web_search_20250305","name":"search","cache_control":{"type":"ephemeral"}}]`)
	require.Equal(t, typed, wrapperAffinityTools(typed))
}
