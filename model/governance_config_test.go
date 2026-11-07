package model

import (
	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/stretchr/testify/require"
	"strings"
	"sync"
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

func TestGovernanceMultiKeySelection(t *testing.T) {
	c := &Channel{Id: 76543, Key: "disabled\nfirst\nsecond", ChannelInfo: ChannelInfo{IsMultiKey: true, MultiKeySize: 3, MultiKeyMode: constant.MultiKeyModePolling, MultiKeyStatusList: map[int]int{0: 2}, MultiKeyPollingIndex: 99}}
	revision, err := GovernanceChannelRevision(c)
	require.NoError(t, err)
	before, err := common.Marshal(c)
	require.NoError(t, err)
	counts := make(chan int, 200)
	var wg sync.WaitGroup
	for range 200 {
		wg.Go(func() {
			key, index, err := GovernanceChannelKey(c, revision)
			if err != nil || (key != "first" && key != "second") {
				counts <- -1
				return
			}
			counts <- index
		})
	}
	wg.Wait()
	close(counts)
	got := map[int]int{}
	for index := range counts {
		got[index]++
	}
	require.Equal(t, map[int]int{1: 100, 2: 100}, got)
	after, err := common.Marshal(c)
	require.NoError(t, err)
	require.Equal(t, string(before), string(after), "scheduler mutated reviewed configuration")
	c.ChannelInfo.MultiKeyMode = constant.MultiKeyModeRandom
	for range 100 {
		key, index, err := GovernanceChannelKey(c, revision)
		require.NoError(t, err)
		require.Contains(t, []int{1, 2}, index)
		require.Contains(t, []string{"first", "second"}, key)
	}
	for name, change := range map[string]func(*Channel){
		"size":     func(c *Channel) { c.ChannelInfo.MultiKeySize = 4 },
		"empty":    func(c *Channel) { c.Key = "disabled\n\nsecond" },
		"header":   func(c *Channel) { c.Key = "disabled\nfirst\r\nsecond" },
		"mode":     func(c *Channel) { c.ChannelInfo.MultiKeyMode = "other" },
		"index":    func(c *Channel) { c.ChannelInfo.MultiKeyStatusList[4] = 1 },
		"disabled": func(c *Channel) { c.ChannelInfo.MultiKeyStatusList = map[int]int{0: 2, 1: 2, 2: 2} },
	} {
		t.Run(name, func(t *testing.T) {
			copy, err := common.DeepCopy(c)
			require.NoError(t, err)
			change(copy)
			_, _, err = GovernanceChannelKey(copy, revision)
			require.Error(t, err)
		})
	}
}
