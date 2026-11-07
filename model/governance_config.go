package model

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math/rand/v2"
	"strings"
	"sync"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
)

// GovernanceChannelSnapshot captures one immutable selected configuration.
// Lock order matches key status/polling writers: polling, then channel cache.
// It does not replace channel selection or grant access to a different channel.
func GovernanceChannelSnapshot(channel *Channel) (*Channel, error) {
	if channel == nil {
		return nil, errors.New("missing governed channel")
	}
	lock := GetChannelPollingLock(channel.Id)
	lock.Lock()
	defer lock.Unlock()
	channelSyncLock.RLock()
	defer channelSyncLock.RUnlock()
	return common.DeepCopy(channel)
}

// GovernanceChannelRevision accepts an owned snapshot, never a live cache pointer.
// The credential keys an HMAC rather than appearing in the public canonical body.
// Rotating any key changes the revision. Operational counters do not.
func GovernanceChannelRevision(channel *Channel) (string, error) {
	if channel == nil || channel.Id <= 0 || len(channel.Key) == 0 || len(channel.Key) > 1<<20 {
		return "", errors.New("invalid governed channel")
	}
	copy := *channel
	copy.Key = ""
	copy.Keys = nil
	copy.Name = ""
	copy.Tag = nil
	copy.Remark = nil
	copy.TestModel = nil
	copy.TestTime = 0
	copy.ResponseTime = 0
	copy.Balance = 0
	copy.BalanceUpdatedTime = 0
	copy.UsedQuota = 0
	copy.ChannelInfo.MultiKeyPollingIndex = 0
	copy.ChannelInfo.MultiKeyDisabledReason = nil
	copy.ChannelInfo.MultiKeyDisabledTime = nil
	base := channel.GetBaseURL()
	if base == "" {
		base = constant.GetChannelBaseURL(channel.Type)
	}
	copy.BaseURL = &base
	// Discovery observations are counters, while its enable/auto-sync settings
	// and the resulting configured model list remain part of the approval.
	settings := map[string]any{}
	if copy.OtherSettings != "" && (common.Unmarshal([]byte(copy.OtherSettings), &settings) != nil || settings == nil) {
		return "", errors.New("invalid governed channel settings")
	}
	delete(settings, "upstream_model_update_last_check_time")
	delete(settings, "upstream_model_update_last_detected_models")
	delete(settings, "upstream_model_update_last_removed_models")
	encoded, err := common.Marshal(settings)
	if err != nil {
		return "", errors.New("invalid governed channel settings")
	}
	copy.OtherSettings = string(encoded)

	raw, err := common.Marshal(copy)
	if err != nil || len(raw) > 256<<10 {
		return "", errors.New("invalid governed channel size")
	}
	hash := hmac.New(sha256.New, []byte(channel.Key))
	_, _ = hash.Write([]byte("aigw-channel-config-v1\x00"))
	_, _ = hash.Write(raw)
	return hex.EncodeToString(hash.Sum(nil)), nil
}

// Scheduling is deliberately independent of native ChannelInfo persistence.
// A revision change or process restart resets local round-robin ordering.
var governanceKeyCursors sync.Map

type governanceKeyCursor struct {
	mu       sync.Mutex
	revision string
	next     int
}

// GovernanceChannelKey selects from an owned, reviewed snapshot without mutating
// channel status, the snapshot, or persistent configuration. No retry is granted.
func GovernanceChannelKey(channel *Channel, revision string) (string, int, error) {
	if channel == nil || channel.Id <= 0 || len(revision) != 64 {
		return "", 0, errors.New("invalid governed key selection")
	}
	if !channel.ChannelInfo.IsMultiKey {
		return channel.Key, 0, nil
	}
	// Governed providers accept newline-separated bearer credentials, not JSON
	// service-account arrays. Never use the cached Keys slice as authority.
	if strings.HasPrefix(strings.TrimSpace(channel.Key), "[") {
		return "", 0, errors.New("unsupported governed key format")
	}
	keys := strings.Split(strings.Trim(channel.Key, "\n"), "\n")
	if len(keys) == 0 || len(keys) > 1024 || channel.ChannelInfo.MultiKeySize != len(keys) {
		return "", 0, errors.New("invalid governed key count")
	}
	enabled := make([]int, 0, len(keys))
	for i, key := range keys {
		if key == "" || strings.ContainsAny(key, " \t\r\n") {
			return "", 0, errors.New("invalid governed key format")
		}
		status, exists := channel.ChannelInfo.MultiKeyStatusList[i]
		if !exists || status == common.ChannelStatusEnabled {
			enabled = append(enabled, i)
		}
	}
	for i := range channel.ChannelInfo.MultiKeyStatusList {
		if i < 0 || i >= len(keys) {
			return "", 0, errors.New("invalid governed key status index")
		}
	}
	if len(enabled) == 0 {
		return "", 0, errors.New("no enabled governed key")
	}
	var index int
	switch channel.ChannelInfo.MultiKeyMode {
	case constant.MultiKeyModeRandom:
		index = enabled[rand.IntN(len(enabled))]
	case constant.MultiKeyModePolling:
		value, _ := governanceKeyCursors.LoadOrStore(channel.Id, &governanceKeyCursor{})
		cursor := value.(*governanceKeyCursor)
		cursor.mu.Lock()
		if cursor.revision != revision {
			cursor.revision = revision
			cursor.next = 0
		}
		index = enabled[cursor.next%len(enabled)]
		cursor.next = (cursor.next + 1) % len(enabled)
		cursor.mu.Unlock()
	default:
		return "", 0, errors.New("unsupported governed key mode")
	}
	return keys[index], index, nil
}
