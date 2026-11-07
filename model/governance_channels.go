package model

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// GovernanceChannelChange is both an immutable audit record and an idempotency
// receipt. It is committed in the same database transaction as the channel.
// Neither the request nor its credential is stored in this table.
type GovernanceChannelChange struct {
	ID             string `gorm:"primaryKey;size:64" json:"operation_id"`
	OwnerID        int    `gorm:"index" json:"-"`
	ActorIssuer    string `gorm:"size:2048" json:"actor_issuer"`
	ActorSubject   string `gorm:"size:256" json:"actor_subject"`
	RequestHash    string `gorm:"size:64" json:"-"`
	ChannelID      int    `gorm:"index" json:"channel_id"`
	BeforeRevision string `gorm:"size:64" json:"before_revision"`
	AfterRevision  string `gorm:"size:64" json:"after_revision"`
	CreatedAt      int64  `json:"created_at"`
}

type GovernanceChannelView struct {
	ID                   int      `json:"id"`
	Name                 string   `json:"name"`
	Type                 int      `json:"type"`
	BaseURL              string   `json:"base_url"`
	Models               []string `json:"models"`
	Enabled              bool     `json:"enabled"`
	CredentialConfigured bool     `json:"credential_configured"`
	KeyCount             int      `json:"key_count"`
	Revision             string   `json:"revision"`
	EditRevision         string   `json:"edit_revision"`
	Editable             bool     `json:"editable"`
}

type GovernanceChannelWrite struct {
	OperationID      string   `json:"operation_id"`
	ActorIssuer      string   `json:"actor_issuer"`
	ActorSubject     string   `json:"actor_subject"`
	ExpectedRevision string   `json:"expected_revision"`
	Name             string   `json:"name"`
	Type             int      `json:"type"`
	BaseURL          string   `json:"base_url"`
	Models           []string `json:"models"`
	Enabled          bool     `json:"enabled"`
	APIKey           *string  `json:"api_key,omitempty"`
}

func (GovernanceChannelWrite) String() string   { return "[channel write; credential redacted]" }
func (GovernanceChannelWrite) GoString() string { return "[channel write; credential redacted]" }

var channelOperationID = regexp.MustCompile(`^[A-Za-z0-9_-]{16,64}$`)
var channelModelName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_./:-]{0,127}$`)

// GovernanceChannelPublic deliberately does not serialize the native Channel:
// native settings, header overrides and raw credentials may contain secrets.
func GovernanceChannelPublic(c *Channel) GovernanceChannelView {
	v := GovernanceChannelView{ID: c.Id, Name: c.Name, Type: c.Type, Models: strings.Split(c.Models, ","), Enabled: c.Status == common.ChannelStatusEnabled, CredentialConfigured: c.Key != "", KeyCount: 1}
	if c.Key == "" {
		v.KeyCount = 0
	} else if c.ChannelInfo.IsMultiKey {
		v.KeyCount = c.ChannelInfo.MultiKeySize
	}
	v.Revision, _ = GovernanceChannelRevision(c)
	digest := sha256.Sum256([]byte(v.Revision + "\x00" + c.Name))
	v.EditRevision = hex.EncodeToString(digest[:])
	u, err := url.Parse(c.GetBaseURL())
	if err == nil && u.User == nil && u.RawQuery == "" && u.Fragment == "" {
		v.BaseURL = c.GetBaseURL()
	}
	v.Editable = (c.Type == constant.ChannelTypeOpenAI || c.Type == constant.ChannelTypeDeepSeek || c.Type == constant.ChannelTypeAnthropic) && v.Revision != "" && v.BaseURL != ""
	// Hidden protocol mutations cannot be reviewed by this deliberately narrow
	// management surface. Preserve them, but require native remediation first.
	for _, field := range []*string{c.OpenAIOrganization, c.ModelMapping, c.StatusCodeMapping, c.Setting, c.ParamOverride, c.HeaderOverride} {
		if field != nil && *field != "" && *field != "{}" {
			v.Editable = false
		}
	}
	if (c.OtherSettings != "" && c.OtherSettings != "{}") || c.Other != "" {
		v.Editable = false
	}
	return v
}

func GovernanceChannels(ctx context.Context, after int) ([]GovernanceChannelView, error) {
	var channels []Channel
	err := DB.WithContext(ctx).Order("id ASC").Where("id > ?", after).Limit(100).Find(&channels).Error
	if err != nil {
		return nil, err
	}
	result := make([]GovernanceChannelView, 0, len(channels))
	for i := range channels {
		result = append(result, GovernanceChannelPublic(&channels[i]))
	}
	return result, nil
}

func GovernanceChannelReceipt(ctx context.Context, owner int, id string) (GovernanceChannelChange, error) {
	var receipt GovernanceChannelChange
	err := DB.WithContext(ctx).Where("id = ? AND owner_id = ?", id, owner).First(&receipt).Error
	return receipt, err
}

// ChangeGovernanceChannel updates only explicitly supported fields. Advanced
// native settings are preserved and remain subject to inference-time review.
func ChangeGovernanceChannel(ctx context.Context, owner, id int, in GovernanceChannelWrite) (GovernanceChannelChange, error) {
	var receipt GovernanceChannelChange
	if owner < 1 || id < 0 || !channelOperationID.MatchString(in.OperationID) || len(in.ActorIssuer) < 1 || len(in.ActorIssuer) > 2048 || len(in.ActorSubject) < 1 || len(in.ActorSubject) > 256 || strings.TrimSpace(in.Name) != in.Name || len(in.Name) < 1 || len(in.Name) > 100 || len(in.Models) < 1 || len(in.Models) > 32 || (in.Type != 1 && in.Type != 14 && in.Type != 43) || (id == 0 && (in.APIKey == nil || in.ExpectedRevision != "")) || (id > 0 && len(in.ExpectedRevision) != 64) {
		return receipt, ErrGovernanceInvalid
	}
	seen := map[string]bool{}
	for _, m := range in.Models {
		if !channelModelName.MatchString(m) || seen[m] {
			return receipt, ErrGovernanceInvalid
		}
		seen[m] = true
	}
	var keys []string
	if in.APIKey != nil {
		if len(*in.APIKey) < 1 || len(*in.APIKey) > 64<<10 {
			return receipt, ErrGovernanceInvalid
		}
		keys = strings.Split(*in.APIKey, "\n")
		if len(keys) > 256 {
			return receipt, ErrGovernanceInvalid
		}
		for _, key := range keys {
			if key == "" || strings.HasPrefix(key, "[") {
				return receipt, ErrGovernanceInvalid
			}
			for _, r := range key {
				if r < 33 || r > 126 {
					return receipt, ErrGovernanceInvalid
				}
			}
		}
	}
	raw, err := common.Marshal(struct {
		ID   int
		Body GovernanceChannelWrite
	}{id, in})
	if err != nil {
		return receipt, ErrGovernanceInvalid
	}
	hash := sha256.Sum256(raw)
	fingerprint := hex.EncodeToString(hash[:])
	if old, e := GovernanceChannelReceipt(ctx, owner, in.OperationID); e == nil {
		if old.RequestHash != fingerprint {
			return receipt, ErrGovernanceConflict
		}
		return old, nil
	} else if !errors.Is(e, gorm.ErrRecordNotFound) {
		return receipt, e
	}
	// Do not let SQL error logging serialize an API key in INSERT/UPDATE values.
	err = DB.WithContext(ctx).Session(&gorm.Session{Logger: logger.Discard}).Transaction(func(tx *gorm.DB) error {
		channel := Channel{Group: "default", CreatedTime: time.Now().Unix()}
		if id > 0 {
			if err := lockForUpdate(tx).First(&channel, "id = ?", id).Error; err != nil {
				return err
			}
			view := GovernanceChannelPublic(&channel)
			if !view.Editable || view.EditRevision != in.ExpectedRevision {
				return ErrGovernanceConflict
			}
			receipt.BeforeRevision = view.Revision
		}
		channel.Name, channel.Type, channel.BaseURL, channel.Models = in.Name, in.Type, &in.BaseURL, strings.Join(in.Models, ",")
		channel.Status = common.ChannelStatusManuallyDisabled
		if in.Enabled {
			channel.Status = common.ChannelStatusEnabled
		}
		if in.APIKey != nil {
			channel.Key = *in.APIKey
			channel.ChannelInfo = ChannelInfo{}
			if len(keys) > 1 {
				channel.ChannelInfo = ChannelInfo{IsMultiKey: true, MultiKeySize: len(keys), MultiKeyMode: constant.MultiKeyMode("polling")}
			}
		}
		if id == 0 {
			if err := tx.Create(&channel).Error; err != nil {
				return err
			}
		} else {
			// Only managed fields are written; operational usage/counters remain intact.
			if err := tx.Model(&Channel{}).Where("id = ?", id).Updates(map[string]any{"name": channel.Name, "type": channel.Type, "base_url": in.BaseURL, "models": channel.Models, "status": channel.Status, "key": channel.Key, "channel_info": channel.ChannelInfo}).Error; err != nil {
				return err
			}
		}
		if err := channel.UpdateAbilities(tx); err != nil {
			return err
		}
		// Read back DB defaults before computing the published commitment.
		if err := tx.First(&channel, "id = ?", channel.Id).Error; err != nil {
			return err
		}
		revision, err := GovernanceChannelRevision(&channel)
		if err != nil {
			return err
		}
		receipt.ID, receipt.OwnerID, receipt.RequestHash, receipt.ChannelID = in.OperationID, owner, fingerprint, channel.Id
		receipt.ActorIssuer, receipt.ActorSubject = in.ActorIssuer, in.ActorSubject
		receipt.AfterRevision, receipt.CreatedAt = revision, time.Now().Unix()
		return tx.Create(&receipt).Error
	})
	if err != nil { // A concurrent replay may have committed first.
		if old, e := GovernanceChannelReceipt(ctx, owner, in.OperationID); e == nil && old.RequestHash == fingerprint {
			return old, nil
		}
		return GovernanceChannelChange{}, err
	}
	InitChannelCache()
	return receipt, nil
}
