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

func TestGovernedDispatchRejectsPostConversionDrift(t *testing.T) {
	for name, change := range map[string]func(*RelayInfo){
		"runtime headers": func(i *RelayInfo) {
			i.RuntimeHeadersOverride = map[string]any{"anthropic-beta": "synthetic paid feature"}
		},
		"different priced model": func(i *RelayInfo) { i.UpstreamModelName = "other-model" },
		"mapping flag":           func(i *RelayInfo) { i.IsModelMapped = true },
		"untrusted TLS":          func(i *RelayInfo) { i.ChannelSetting.TLSInsecureSkipVerify = true },
	} {
		t.Run(name, func(t *testing.T) {
			info := &RelayInfo{OriginModelName: "approved", ChannelMeta: &ChannelMeta{UpstreamModelName: "approved"}}
			require.NoError(t, ValidateGovernedDispatchConfig(info))
			change(info)
			require.Error(t, ValidateGovernedDispatchConfig(info))
		})
	}
	require.Error(t, ValidateGovernedDispatchConfig(nil))
}

func TestGovernedEmbeddingInputBounds(t *testing.T) {
	for _, body := range []string{`{"model":"approved","input":"synthetic"}`, `{"model":"approved","input":["one","two"],"dimensions":3,"encoding_format":"float"}`} {
		count, err := GovernedEmbeddingEstimate([]byte(body), "approved")
		require.NoError(t, err)
		require.Positive(t, count)
	}
	for _, body := range []string{`{"model":"other","input":"synthetic"}`, `{"model":"approved","input":[1,2]}`, `{"model":"approved","input":["one",3]}`, `{"model":"approved","input":""}`, `{"model":"approved","input":"x","encoding_format":"base64"}`, `{"model":"approved","input":"x","dimensions":4097}`, `{"model":"approved","input":"x","max_tokens":8}`} {
		_, err := GovernedEmbeddingEstimate([]byte(body), "approved")
		require.Error(t, err)
	}
}
