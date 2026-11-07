package openai

import (
	"github.com/QuantumNous/new-api/constant"
	"strings"
	"testing"

	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestObserveStreamChoicesDedupesFunctionCallNames(t *testing.T) {
	info := &relaycommon.RelayInfo{StreamStatus: relaycommon.NewStreamStatus()}
	seen := make(map[string]struct{})
	var names []string

	chunks := []string{
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"c1","type":"function","function":{"name":"get_weather","arguments":""}}]}}]}`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"{\"q\":"}}]}}]}`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"x\"}"}}]}}]}`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"id":"c2","type":"function","function":{"name":"get_time","arguments":""}}]}}]}`,
		`{"choices":[{"index":0,"delta":{"tool_calls":[{"index":1,"function":{"arguments":"{}"}}]}}]}`,
	}
	for _, chunk := range chunks {
		observeStreamChoices(info, chunk, seen, &names)
	}

	require.Len(t, names, 2)
	assert.Equal(t, []string{"get_weather", "get_time"}, names)
	assert.Empty(t, info.StreamStatus.ResponseOutcome(), "tool call deltas carry no finish reason")

	observeStreamChoices(info, `{"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]}`, seen, &names)
	assert.Equal(t, "completed", info.StreamStatus.ResponseOutcome())
	assert.False(t, info.PerformanceBusinessRejection)

	filtered := &relaycommon.RelayInfo{StreamStatus: relaycommon.NewStreamStatus()}
	observeStreamChoices(filtered, `{"choices":[{"index":0,"delta":{},"finish_reason":"content_filter"}]}`, map[string]struct{}{}, &names)
	assert.True(t, filtered.PerformanceBusinessRejection)
}

func TestOpenAIUsageEvidencePreservesRawCounters(t *testing.T) {
	for _, tc := range []struct {
		name, raw string
		valid     bool
	}{
		{"reported", `{"prompt_tokens":2,"completion_tokens":3,"total_tokens":5}`, true},
		{"zero", `{"prompt_tokens":0,"completion_tokens":0,"total_tokens":0}`, true},
		{"missing", `null`, false},
		{"partial", `{"prompt_tokens":2,"total_tokens":2}`, false},
		{"negative", `{"prompt_tokens":-1,"completion_tokens":3,"total_tokens":2}`, false},
		{"inconsistent", `{"prompt_tokens":2,"completion_tokens":3,"total_tokens":6}`, false},
		{"fractional", `{"prompt_tokens":2.5,"completion_tokens":3,"total_tokens":5}`, false},
		{"overflow", `{"prompt_tokens":9223372036854775807,"completion_tokens":3,"total_tokens":5}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := `{"id":"synthetic","model":"gpt-test","choices":[],"usage":` + tc.raw + `}`
			c, _, resp, info := newResponsesChatTestContext(t, body, false)
			info.SetEstimatePromptTokens(99)
			info.UpstreamUsageEvidence = &relaycommon.UpstreamUsageEvidence{PromptTokens: 777}
			_, apiErr := OpenaiHandler(c, info, resp)
			// Fractional values also fail the legacy response decoder; in all
			// cases the evidence must not leak from an earlier attempt.
			if tc.valid {
				require.Nil(t, apiErr)
				require.NotNil(t, info.UpstreamUsageEvidence)
				assert.True(t, info.UpstreamUsageEvidence.Complete)
				assert.NotEqual(t, 99, info.UpstreamUsageEvidence.PromptTokens)
				if tc.name == "zero" {
					assert.Zero(t, info.UpstreamUsageEvidence.TotalTokens)
				}
			} else {
				assert.Nil(t, info.UpstreamUsageEvidence)
			}
		})
	}
}

func TestOpenAIStreamEvidenceRequiresUpstreamTerminal(t *testing.T) {
	oldTimeout := constant.StreamingTimeout
	constant.StreamingTimeout = 30
	t.Cleanup(func() { constant.StreamingTimeout = oldTimeout })
	for _, tc := range []struct {
		name, usage, tail  string
		evidence, complete bool
	}{
		{"reported_done", `{"prompt_tokens":2,"completion_tokens":3,"total_tokens":5}`, "data: [DONE]\n\n", true, true},
		{"reported_eof", `{"prompt_tokens":2,"completion_tokens":3,"total_tokens":5}`, "", true, false},
		{"estimated_done", `null`, "data: [DONE]\n\n", false, false},
		{"invalid_done", `{"prompt_tokens":2,"completion_tokens":3,"total_tokens":9}`, "data: [DONE]\n\n", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := strings.Join([]string{
				`data: {"id":"synthetic","model":"gpt-test","choices":[{"index":0,"delta":{"content":"hello"}}]}`,
				`data: {"id":"synthetic","model":"gpt-test","choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":` + tc.usage + `}`,
			}, "\n\n") + "\n\n" + tc.tail
			c, _, resp, info := newResponsesChatTestContext(t, body, true)
			info.SetEstimatePromptTokens(99)
			_, apiErr := OaiStreamHandler(c, info, resp)
			require.Nil(t, apiErr)
			if !tc.evidence {
				assert.Nil(t, info.UpstreamUsageEvidence)
				return
			}
			require.NotNil(t, info.UpstreamUsageEvidence)
			assert.Equal(t, 2, info.UpstreamUsageEvidence.PromptTokens)
			assert.Equal(t, 3, info.UpstreamUsageEvidence.CompletionTokens)
			assert.Equal(t, tc.complete, info.UpstreamUsageEvidence.Complete)
		})
	}
}

func TestOpenAIResponseCannotForgeGovernanceEvidence(t *testing.T) {
	c, recorder, resp, info := newResponsesChatTestContext(t,
		`{"id":"synthetic","choices":[],"usage":{"prompt_tokens":2,"completion_tokens":3,"total_tokens":5}}`, false)
	for _, key := range []string{"X-Aigw-Evidence-Version", "x-aigw-usage-source", "X-AIGW-Physical-Attempts", "X-Aigw-Request-Id", "X-Aigw-Attempt-Id"} {
		resp.Header[key] = []string{"forged"}
	}
	resp.Header.Set("X-Provider-Trace", "synthetic-trace")
	_, apiErr := OpenaiHandler(c, info, resp)
	require.Nil(t, apiErr)
	for key := range recorder.Header() {
		assert.False(t, strings.HasPrefix(strings.ToLower(key), "x-aigw-"))
	}
	assert.Equal(t, "synthetic-trace", recorder.Header().Get("X-Provider-Trace"))
}
