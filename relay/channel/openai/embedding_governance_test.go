package openai

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGovernedEmbeddingRawUsage(t *testing.T) {
	valid := []byte(`{"usage":{"prompt_tokens":2,"total_tokens":2}}`)
	require.NotNil(t, captureEmbeddingUsage(valid, true))
	require.Nil(t, captureUpstreamUsage(valid, true), "embedding omission must not weaken Chat evidence")
	for _, raw := range []string{`{}`, `{"usage":{"prompt_tokens":0,"total_tokens":0}}`, `{"usage":{"prompt_tokens":2,"total_tokens":3}}`, `{"usage":{"prompt_tokens":2,"total_tokens":2,"completion_tokens":null}}`, `{"usage":{"prompt_tokens":2,"total_tokens":2,"completion_tokens":1}}`, `{"usage":{"prompt_tokens":2,"total_tokens":2,"image_count":1}}`, `{"usage":{"prompt_tokens":2.5,"total_tokens":2.5}}`} {
		require.Nil(t, captureEmbeddingUsage([]byte(raw), true))
	}
}
