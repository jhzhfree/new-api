package common

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGovernedOutputBound(t *testing.T) {
	for _, body := range []string{`{"max_tokens":8}`, `{"max_tokens":4}`, `{"max_completion_tokens":8}`} {
		require.NoError(t, ValidateGovernedOutputBound([]byte(body), 8))
	}
	for _, body := range []string{`{}`, `{"max_tokens":9}`, `{"max_tokens":0}`, `{"max_tokens":-1}`, `{"max_tokens":1.5}`, `{"max_tokens":"8"}`, `{"max_tokens":8,"max_completion_tokens":8}`, `{"max_tokens":18446744073709551615}`} {
		require.Error(t, ValidateGovernedOutputBound([]byte(body), 8))
	}
}
