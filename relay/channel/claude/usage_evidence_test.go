package claude

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestClaudeRawUsageEvidence(t *testing.T) {
	start := `{"type":"message_start","message":{"usage":{"input_tokens":2,"output_tokens":0,"cache_read_input_tokens":4,"cache_creation_input_tokens":5}}}`
	delta := `{"type":"message_delta","usage":{"output_tokens":3}}`
	stop := `{"type":"message_stop"}`
	for _, tc := range []struct {
		name   string
		frames []string
		valid  bool
	}{
		{"complete", []string{start, delta, stop}, true},
		{"client-tool", []string{start, `{"type":"content_block_start","content_block":{"type":"tool_use"}}`, `{"type":"content_block_delta","delta":{"type":"input_json_delta"}}`, delta, stop}, true},
		{"hosted-tool-without-count", []string{start, `{"type":"content_block_start","content_block":{"type":"server_tool_use"}}`, delta, stop}, false},
		{"cumulative", []string{start, `{"type":"message_delta","usage":{"output_tokens":2}}`, delta, stop}, true},
		{"no-stop", []string{start, delta}, false},
		{"no-final-usage", []string{start, stop}, false},
		{"duplicate-start", []string{start, start, delta, stop}, false},
		{"regression", []string{start, delta, `{"type":"message_delta","usage":{"output_tokens":2}}`, stop}, false},
		{"error", []string{start, delta, `{"type":"error"}`, stop}, false},
		{"post-stop", []string{start, delta, stop, delta}, false},
		{"missing-input", []string{`{"type":"message_start","message":{"usage":{"output_tokens":0}}}`, delta, stop}, false},
		{"negative", []string{start, `{"type":"message_delta","usage":{"output_tokens":-1}}`, stop}, false},
		{"tool-charge", []string{start, `{"type":"message_delta","usage":{"output_tokens":3,"server_tool_use":{"web_search_requests":1}}}`, stop}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			evidence := &claudeUsageEvidence{}
			for _, frame := range tc.frames {
				evidence.observe([]byte(frame))
			}
			value := evidence.result(true)
			if tc.valid {
				require.NotNil(t, value)
				require.Equal(t, 11, value.PromptTokens)
				require.Equal(t, 3, value.CompletionTokens)
				require.Equal(t, 14, value.TotalTokens)
			} else {
				require.Nil(t, value)
			}
			require.Nil(t, evidence.result(false))
		})
	}
	require.NotNil(t, claudeJSONEvidence([]byte(`{"type":"message","stop_reason":"end_turn","usage":{"input_tokens":2,"output_tokens":3}}`)))
	for _, raw := range []string{`{"type":"message","stop_reason":"end_turn","content":[{"type":"server_tool_use"}],"usage":{"input_tokens":2,"output_tokens":3}}`, `{"type":"message","usage":{"input_tokens":2,"output_tokens":3}}`, `{"type":"message","stop_reason":"end_turn","usage":{"input_tokens":2}}`, `{"type":"message","stop_reason":"end_turn","usage":{"input_tokens":1e30,"output_tokens":3}}`} {
		require.Nil(t, claudeJSONEvidence([]byte(raw)))
	}
}

func TestClaudeCacheWriteTTLEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, creation string
		known          bool
		five, hour     int64
	}{
		{"mixed", `,"cache_creation":{"ephemeral_5m_input_tokens":3,"ephemeral_1h_input_tokens":2}`, true, 3, 2},
		{"missing", "", false, 0, 0},
		{"partial", `,"cache_creation":{"ephemeral_5m_input_tokens":5}`, false, 0, 0},
		{"mismatch", `,"cache_creation":{"ephemeral_5m_input_tokens":3,"ephemeral_1h_input_tokens":3}`, false, 0, 0},
		{"negative", `,"cache_creation":{"ephemeral_5m_input_tokens":-1,"ephemeral_1h_input_tokens":6}`, false, 0, 0},
		{"oversized", `,"cache_creation":{"ephemeral_5m_input_tokens":1000000001,"ephemeral_1h_input_tokens":0}`, false, 0, 0},
		{"unknown-duration", `,"cache_creation":{"ephemeral_5m_input_tokens":3,"ephemeral_1h_input_tokens":2,"future":0}`, false, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			usage := fmt.Sprintf(`{"input_tokens":2,"output_tokens":3,"cache_creation_input_tokens":5%s}`, tc.creation)
			v := claudeJSONEvidence([]byte(`{"type":"message","stop_reason":"end_turn","usage":` + usage + `}`))
			invalid := tc.name == "negative" || tc.name == "oversized" || tc.name == "mismatch"
			if invalid {
				require.Nil(t, v)
			} else {
				require.NotNil(t, v)
				require.Equal(t, tc.known, v.CacheWriteTTLKnown)
			}
			if tc.known {
				require.Equal(t, tc.five, v.CacheWrite5mTokens)
				require.Equal(t, tc.hour, v.CacheWrite1hTokens)
			}
			e := &claudeUsageEvidence{}
			e.observe([]byte(`{"type":"message_start","message":{"usage":` + usage + `}}`))
			e.observe([]byte(`{"type":"message_delta","usage":{"output_tokens":4}}`))
			e.observe([]byte(`{"type":"message_stop"}`))
			streamed := e.result(true)
			if invalid {
				require.Nil(t, streamed)
			} else {
				require.NotNil(t, streamed)
				require.Equal(t, tc.known, streamed.CacheWriteTTLKnown)
			}
		})
	}
	zero := `{"type":"message","stop_reason":"end_turn","usage":{"input_tokens":2,"output_tokens":3,"cache_creation_input_tokens":0}}`
	require.True(t, claudeJSONEvidence([]byte(zero)).CacheWriteTTLKnown)
	contradictory := strings.Replace(zero, `"cache_creation_input_tokens":0`, `"cache_creation_input_tokens":0,"cache_creation":{"ephemeral_5m_input_tokens":1,"ephemeral_1h_input_tokens":0}`, 1)
	require.Nil(t, claudeJSONEvidence([]byte(contradictory)))
}

func TestClaudeInitialEvidenceIsNotSettlement(t *testing.T) {
	e := &claudeUsageEvidence{}
	require.Nil(t, e.initial())
	e.observe([]byte(`{"type":"message_start","message":{"usage":{"input_tokens":2,"output_tokens":1,"cache_read_input_tokens":4,"cache_creation_input_tokens":5}}}`))
	initial := e.initial()
	require.NotNil(t, initial)
	require.True(t, initial.Valid(128, 8))
	require.Nil(t, e.result(true))
	require.False(t, initial.Matches(e.result(true)))
	require.False(t, initial.Valid(10, 8))
	require.False(t, initial.Valid(128, 0))
	e.observe([]byte(`{"type":"message_delta","usage":{"output_tokens":3}}`))
	require.Nil(t, e.initial())
	e.observe([]byte(`{"type":"message_stop"}`))
	require.True(t, initial.Matches(e.result(true)))
	require.EqualValues(t, 1, initial.OutputTokens)
	e.input++
	require.False(t, initial.Matches(e.result(true)))
	for _, usage := range []string{`{"output_tokens":0}`, `{"input_tokens":2,"output_tokens":-1}`, `{"input_tokens":2,"output_tokens":0,"cache_creation_input_tokens":-1}`} {
		invalid := &claudeUsageEvidence{}
		invalid.observe([]byte(`{"type":"message_start","message":{"usage":` + usage + `}}`))
		require.Nil(t, invalid.initial())
	}
}
