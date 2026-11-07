package model

import (
	"github.com/QuantumNous/new-api/common"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestGovernanceChannelRevision(t *testing.T) {
	base := "https://synthetic.example"
	channel := &Channel{Id: 1, Type: 1, Key: "synthetic-key", Status: 1, Models: "reviewed", Group: "default", BaseURL: &base}
	original, err := GovernanceChannelRevision(channel)
	require.NoError(t, err)
	require.Len(t, original, 64)
	for name, change := range map[string]func(*Channel){
		"key":        func(c *Channel) { c.Key = "rotated-key" },
		"endpoint":   func(c *Channel) { s := "https://other.example"; c.BaseURL = &s },
		"models":     func(c *Channel) { c.Models = "other" },
		"status":     func(c *Channel) { c.Status = 2 },
		"group":      func(c *Channel) { c.Group = "other" },
		"provider":   func(c *Channel) { c.Type = 43 },
		"mapping":    func(c *Channel) { s := `{"reviewed":"other"}`; c.ModelMapping = &s },
		"header":     func(c *Channel) { s := `{"x-test":"value"}`; c.HeaderOverride = &s },
		"params":     func(c *Channel) { s := `{"temperature":0}`; c.ParamOverride = &s },
		"settings":   func(c *Channel) { s := `{"tls_insecure_skip_verify":true}`; c.Setting = &s },
		"extra":      func(c *Channel) { c.OtherSettings = `{"future_config":true}` },
		"key_status": func(c *Channel) { c.ChannelInfo.MultiKeyStatusList = map[int]int{0: 2} },
	} {
		t.Run(name, func(t *testing.T) {
			copy, err := common.DeepCopy(channel)
			require.NoError(t, err)
			change(copy)
			revision, err := GovernanceChannelRevision(copy)
			require.NoError(t, err)
			require.NotEqual(t, original, revision)
		})
	}
	snapshot, err := GovernanceChannelSnapshot(channel)
	require.NoError(t, err)
	snapshot.Name = "label"
	snapshot.UsedQuota = 100
	snapshot.Balance = 23
	snapshot.TestTime = 123
	snapshot.ResponseTime = 10
	snapshot.ChannelInfo.MultiKeyPollingIndex = 2
	snapshot.OtherSettings = `{"upstream_model_update_last_check_time":123}`
	stable, err := GovernanceChannelRevision(snapshot)
	require.NoError(t, err)
	require.Equal(t, original, stable)
	snapshot.Key = "changed"
	require.Equal(t, "synthetic-key", channel.Key)
	for _, invalid := range []*Channel{nil, {}, {Id: 1, Key: strings.Repeat("x", (1<<20)+1)}, {Id: 1, Key: "x", OtherSettings: `null`}} {
		_, err := GovernanceChannelRevision(invalid)
		require.Error(t, err)
	}
}
