package claude

import (
	"github.com/stretchr/testify/require"
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
