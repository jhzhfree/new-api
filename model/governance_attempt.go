package model

import (
	"context"
	"errors"
	"regexp"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	GovernancePrepared   = "prepared"
	GovernanceDispatched = "dispatched"
	GovernanceCompleted  = "completed"
	GovernanceCancelled  = "cancelled"
)

var ErrGovernanceConflict = errors.New("governance attempt conflict")
var ErrGovernanceInvalid = errors.New("invalid governance attempt")
var governanceID = regexp.MustCompile(`^[A-Za-z0-9_-]{16,64}$`)
var governanceDigest = regexp.MustCompile(`^[a-f0-9]{64}$`)

// GovernanceAttempt stores execution facts, never credentials or prompt content.
// Attempt IDs are permanent fences: do not purge them without an admission-expiry
// protocol that prevents late requests from recreating a deleted attempt.
type GovernanceAttempt struct {
	ID               string `gorm:"size:64;primaryKey"`
	OwnerID          int    `gorm:"not null;index"`
	RequestID        string `gorm:"size:64;not null"`
	Fingerprint      string `gorm:"size:64;not null"`
	State            string `gorm:"size:16;not null"`
	ChannelID        int
	Model            string `gorm:"size:128"`
	PromptTokens     int64
	CompletionTokens int64
	TotalTokens      int64
	CreatedAt        int64 `gorm:"autoCreateTime:milli"`
	UpdatedAt        int64 `gorm:"autoUpdateTime:milli"`
}

// GovernanceEvent is the durable outbox. A consumer acknowledges it only after
// committing its own idempotent ledger transaction. Delivery is at least once.
type GovernanceEvent struct {
	AttemptID        string `gorm:"size:64;primaryKey;index:idx_governance_pending,priority:4"`
	OwnerID          int    `gorm:"not null;index:idx_governance_pending,priority:1"`
	RequestID        string `gorm:"size:64;not null"`
	State            string `gorm:"size:16;not null"`
	ChannelID        int
	Model            string `gorm:"size:128"`
	PromptTokens     int64
	CompletionTokens int64
	TotalTokens      int64
	CreatedAt        int64 `gorm:"autoCreateTime:milli;index:idx_governance_pending,priority:3"`
	AcknowledgedAt   int64 `gorm:"not null;index:idx_governance_pending,priority:2"`
}

type GovernanceBinding struct {
	OwnerID     int
	RequestID   string
	AttemptID   string
	Fingerprint string
}

func (b GovernanceBinding) valid() bool {
	return b.OwnerID > 0 && governanceID.MatchString(b.RequestID) && governanceID.MatchString(b.AttemptID) && governanceDigest.MatchString(b.Fingerprint)
}

// GovernanceJournal is deliberately not an authentication boundary. Its caller
// must derive the stable owner from verified service identity, never caller JSON.
// Owner IDs identify service principals, not rotating credential/token IDs.
type GovernanceJournal struct{ db *gorm.DB }

func NewGovernanceJournal(db *gorm.DB) (*GovernanceJournal, error) {
	if db == nil {
		return nil, ErrGovernanceInvalid
	}
	return &GovernanceJournal{db: db}, nil
}

func (j *GovernanceJournal) Prepare(ctx context.Context, b GovernanceBinding) error {
	if !b.valid() {
		return ErrGovernanceInvalid
	}
	// Do not upsert: even an identical replay must never reacquire admission.
	return j.db.WithContext(ctx).Create(&GovernanceAttempt{ID: b.AttemptID, OwnerID: b.OwnerID, RequestID: b.RequestID, Fingerprint: b.Fingerprint, State: GovernancePrepared}).Error
}

// Dispatch must commit successfully before any upstream I/O. A crash after this
// transition remains dispatched/unknown, never automatically refunded/retried.
func (j *GovernanceJournal) Dispatch(ctx context.Context, b GovernanceBinding, channelID int, model string) error {
	if !b.valid() || channelID <= 0 || len(model) == 0 || len(model) > 128 {
		return ErrGovernanceInvalid
	}
	result := j.db.WithContext(ctx).Model(&GovernanceAttempt{}).
		Where("id = ? AND owner_id = ? AND request_id = ? AND fingerprint = ? AND state = ?", b.AttemptID, b.OwnerID, b.RequestID, b.Fingerprint, GovernancePrepared).
		Updates(map[string]any{"state": GovernanceDispatched, "channel_id": channelID, "model": model})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return ErrGovernanceConflict
	}
	return nil
}

func (j *GovernanceJournal) Complete(ctx context.Context, b GovernanceBinding, prompt, completion, total int64) error {
	const maxTokens = int64(1<<31 - 1)
	if !b.valid() || prompt < 0 || completion < 0 || total < 0 || prompt > maxTokens || completion > maxTokens || total > maxTokens || prompt+completion != total {
		return ErrGovernanceInvalid
	}
	return j.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var a GovernanceAttempt
		if err := lockForUpdate(tx).Where("id = ?", b.AttemptID).First(&a).Error; err != nil {
			return err
		}
		if a.OwnerID != b.OwnerID || a.RequestID != b.RequestID || a.Fingerprint != b.Fingerprint {
			return ErrGovernanceConflict
		}
		if a.State == GovernanceCompleted {
			if a.PromptTokens == prompt && a.CompletionTokens == completion && a.TotalTokens == total {
				return nil
			}
			return ErrGovernanceConflict
		}
		if a.State != GovernanceDispatched {
			return ErrGovernanceConflict
		}
		result := tx.Model(&GovernanceAttempt{}).Where("id = ? AND state = ?", a.ID, GovernanceDispatched).
			Updates(map[string]any{"state": GovernanceCompleted, "prompt_tokens": prompt, "completion_tokens": completion, "total_tokens": total})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return ErrGovernanceConflict
		}
		return tx.Create(&GovernanceEvent{AttemptID: a.ID, OwnerID: a.OwnerID, RequestID: a.RequestID, State: GovernanceCompleted, ChannelID: a.ChannelID, Model: a.Model, PromptTokens: prompt, CompletionTokens: completion, TotalTokens: total}).Error
	})
}

// Cancel also fences an attempt not yet admitted. Thus a delayed Prepare cannot
// dispatch after the control plane has received a durable no-dispatch outcome.
func (j *GovernanceJournal) Cancel(ctx context.Context, b GovernanceBinding) error {
	if !b.valid() {
		return ErrGovernanceInvalid
	}
	return j.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		proposed := GovernanceAttempt{ID: b.AttemptID, OwnerID: b.OwnerID, RequestID: b.RequestID, Fingerprint: b.Fingerprint, State: GovernanceCancelled}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&proposed).Error; err != nil {
			return err
		}
		var a GovernanceAttempt
		if err := lockForUpdate(tx).Where("id = ?", b.AttemptID).First(&a).Error; err != nil {
			return err
		}
		if a.OwnerID != b.OwnerID || a.RequestID != b.RequestID || a.Fingerprint != b.Fingerprint {
			return ErrGovernanceConflict
		}
		if a.State != GovernancePrepared && a.State != GovernanceCancelled {
			return ErrGovernanceConflict
		}
		if a.State == GovernancePrepared {
			result := tx.Model(&GovernanceAttempt{}).Where("id = ? AND state = ?", a.ID, GovernancePrepared).Update("state", GovernanceCancelled)
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected != 1 {
				return ErrGovernanceConflict
			}
		}
		return tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&GovernanceEvent{AttemptID: a.ID, OwnerID: a.OwnerID, RequestID: a.RequestID, State: GovernanceCancelled}).Error
	})
}

func (j *GovernanceJournal) Get(ctx context.Context, owner int, id string) (*GovernanceAttempt, error) {
	if owner <= 0 || !governanceID.MatchString(id) {
		return nil, ErrGovernanceInvalid
	}
	var a GovernanceAttempt
	err := j.db.WithContext(ctx).Where("id = ? AND owner_id = ?", id, owner).First(&a).Error
	if err != nil {
		return nil, err
	}
	return &a, nil
}

func (j *GovernanceJournal) Pending(ctx context.Context, owner, limit int) ([]GovernanceEvent, error) {
	if owner <= 0 || limit < 1 || limit > 100 {
		return nil, ErrGovernanceInvalid
	}
	var events []GovernanceEvent
	err := j.db.WithContext(ctx).Where("owner_id = ? AND acknowledged_at = 0", owner).Order("created_at, attempt_id").Limit(limit).Find(&events).Error
	return events, err
}

func (j *GovernanceJournal) Acknowledge(ctx context.Context, owner int, id string) error {
	if owner <= 0 || !governanceID.MatchString(id) {
		return ErrGovernanceInvalid
	}
	var event GovernanceEvent
	if err := j.db.WithContext(ctx).Where("attempt_id = ? AND owner_id = ?", id, owner).First(&event).Error; err != nil {
		return err
	}
	return j.db.WithContext(ctx).Model(&GovernanceEvent{}).Where("attempt_id = ? AND owner_id = ? AND acknowledged_at = 0", id, owner).Update("acknowledged_at", time.Now().UnixMilli()).Error
}
