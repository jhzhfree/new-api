package openai

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGovernedOriginalCacheEvidence(t *testing.T) {
	for _, tc := range []struct {
		extra       string
		known       bool
		read, write int64
		valid       bool
	}{
		{``, false, 0, 0, true},
		{`,"prompt_tokens_details":{"cached_tokens":0}`, true, 0, 0, true},
		{`,"prompt_tokens_details":{"cached_tokens":4,"cached_creation_tokens":5}`, true, 4, 5, true},
		{`,"prompt_cache_hit_tokens":4,"prompt_cache_miss_tokens":7`, true, 4, 0, true},
		{`,"prompt_cache_hit_tokens":4,"prompt_cache_miss_tokens":6`, false, 0, 0, false},
		{`,"prompt_tokens_details":{"cached_tokens":3},"prompt_cache_hit_tokens":4,"prompt_cache_miss_tokens":7`, false, 0, 0, false},
		{`,"prompt_tokens_details":{"cached_tokens":-1}`, false, 0, 0, false},
		{`,"prompt_tokens_details":{"cached_tokens":12}`, false, 0, 0, false},
		{`,"prompt_tokens_details":{"cached_tokens":4,"cached_creation_tokens":8}`, false, 0, 0, false},
		{`,"prompt_tokens_details":{"cached_creation_tokens":1}`, false, 0, 0, false},
		{`,"prompt_tokens_details":{"cached_tokens":1.1}`, false, 0, 0, false},
	} {
		t.Run(tc.extra, func(t *testing.T) {
			value := captureUpstreamUsage([]byte(`{"usage":{"prompt_tokens":11,"completion_tokens":3,"total_tokens":14`+tc.extra+`}}`), true)
			if !tc.valid {
				require.Nil(t, value)
				return
			}
			require.NotNil(t, value)
			require.Equal(t, tc.known, value.CacheDetailsVersion == 1)
			require.Equal(t, tc.read, value.CacheReadTokens)
			require.Equal(t, tc.write, value.CacheWriteTokens)
		})
	}
}
