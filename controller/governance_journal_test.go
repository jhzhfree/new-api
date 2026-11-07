package controller

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/QuantumNous/new-api/relay/channel"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/gin-gonic/gin"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestGovernanceJournalDatabaseMatrix(t *testing.T) {
	for _, dialect := range []struct{ kind, env string }{{"sqlite", ""}, {"mysql", "TEST_MYSQL_DSN"}, {"postgres", "TEST_POSTGRES_DSN"}} {
		for _, upgrade := range []bool{false, true} {
			name := dialect.kind + "/fresh"
			if upgrade {
				name = dialect.kind + "/baseline_upgrade"
			}
			t.Run(name, func(t *testing.T) {
				if dialect.env != "" && os.Getenv(dialect.env) == "" {
					t.Skip("isolated database not configured")
				}
				baseline := os.Getenv("TEST_GOVERNANCE_BASELINE_BINARY")
				if upgrade && baseline == "" {
					t.Skip("baseline migration binary not configured")
				}
				_, dsn := newAuditTestDatabase(t, dialect.kind, os.Getenv(dialect.env))
				sqlitePath := ""
				if dialect.kind == "sqlite" {
					sqlitePath = dsn
					dsn = "local"
				}
				t.Setenv("SQL_DSN", dsn)
				t.Setenv("LOG_SQL_DSN", "")
				t.Setenv("GOVERNANCE_SQLITE_PATH", sqlitePath)
				if upgrade {
					command := exec.CommandContext(t.Context(), baseline)
					command.Env = os.Environ()
					// Never include process output/DSNs in assertions. The baseline binary
					// only migrates its isolated database and writes a synthetic sentinel.
					require.NoError(t, command.Run(), "baseline schema initialization")
				}
				oldDB, oldLogDB := model.DB, model.LOG_DB
				oldMaster, oldSQLite := common.IsMasterNode, common.SQLitePath
				oldMain, oldLog := common.MainDatabaseType(), common.LogDatabaseType()
				common.IsMasterNode = true
				common.SQLitePath = sqlitePath
				t.Cleanup(func() {
					if model.DB != nil && model.DB != oldDB {
						if db, err := model.DB.DB(); err == nil {
							_ = db.Close()
						}
					}
					model.DB, model.LOG_DB = oldDB, oldLogDB
					common.IsMasterNode, common.SQLitePath = oldMaster, oldSQLite
					common.SetDatabaseTypes(oldMain, oldLog)
				})
				require.NoError(t, model.InitDB())
				configChannel := model.Channel{Type: 1, Key: "synthetic-config-key", Models: "reviewed", Group: "default", Status: 1}
				require.NoError(t, model.DB.Create(&configChannel).Error)
				persisted, err := model.GetChannelById(configChannel.Id, true)
				require.NoError(t, err)
				revision, err := model.GovernanceChannelRevision(persisted)
				require.NoError(t, err)
				snapshot, err := model.GovernanceChannelSnapshot(persisted)
				require.NoError(t, err)
				selectedRevision, err := model.GovernanceChannelRevision(snapshot)
				require.NoError(t, err)
				require.Equal(t, revision, selectedRevision)
				require.NoError(t, model.DB.Model(&configChannel).Update("used_quota", 100).Error)
				persisted, err = model.GetChannelById(configChannel.Id, true)
				require.NoError(t, err)
				unchanged, err := model.GovernanceChannelRevision(persisted)
				require.NoError(t, err)
				require.Equal(t, revision, unchanged)
				require.NoError(t, model.DB.Model(&configChannel).Update("key", "synthetic-rotated").Error)
				persisted, err = model.GetChannelById(configChannel.Id, true)
				require.NoError(t, err)
				changed, err := model.GovernanceChannelRevision(persisted)
				require.NoError(t, err)
				require.NotEqual(t, revision, changed)

				require.True(t, model.DB.Migrator().HasTable(&model.GovernanceAttempt{}))
				require.True(t, model.DB.Migrator().HasTable(&model.GovernanceEvent{}))
				require.True(t, model.DB.Migrator().HasTable(&model.GovernanceChannelChange{}))
				indexes, err := model.DB.Migrator().GetIndexes(&model.GovernanceEvent{})
				require.NoError(t, err)
				foundPendingIndex := false
				for _, index := range indexes {
					if index.Name() == "idx_governance_pending" {
						foundPendingIndex = true
						if dialect.kind == "postgres" {
							// The PostgreSQL driver's GetIndexes query does not preserve indkey order.
							var definition string
							require.NoError(t, model.DB.Raw("SELECT indexdef FROM pg_indexes WHERE schemaname = current_schema() AND indexname = ?", "idx_governance_pending").Scan(&definition).Error)
							assert.Contains(t, definition, "(owner_id, acknowledged_at, created_at, attempt_id)")
						} else {
							assert.Equal(t, []string{"owner_id", "acknowledged_at", "created_at", "attempt_id"}, index.Columns())
						}
					}
				}
				require.True(t, foundPendingIndex, "bounded owner-scoped outbox scan requires its ordered index")

				journal, err := model.NewGovernanceJournal(model.DB)
				require.NoError(t, err)
				ctx := context.Background()
				var marker model.Option
				markerErr := model.DB.Where(map[string]any{"key": "governance_cache_upgrade_marker"}).Take(&marker).Error
				if markerErr == nil {
					old, err := journal.Get(ctx, 94, "UpgradeTTLAttempt1234")
					require.NoError(t, err)
					require.Equal(t, 1, old.CacheDetailsVersion)
					require.EqualValues(t, 5, old.CacheWriteTokens)
					require.Zero(t, old.CacheWrite5mTokens)
					require.Zero(t, old.CacheWrite1hTokens)
					oldEvents, err := journal.Pending(ctx, 94, 100)
					require.NoError(t, err)
					require.Len(t, oldEvents, 1)
					require.EqualValues(t, 14, oldEvents[0].TotalTokens)
					require.EqualValues(t, 4, oldEvents[0].CacheReadTokens)
					require.Zero(t, oldEvents[0].CacheWrite5mTokens)
					require.Zero(t, oldEvents[0].CacheWrite1hTokens)
				} else {
					require.ErrorIs(t, markerErr, gorm.ErrRecordNotFound)
				}

				binding := model.GovernanceBinding{OwnerID: 7, AttemptID: strings.Repeat("a", 32), RequestID: strings.Repeat("b", 32), Fingerprint: strings.Repeat("c", 64)}
				require.NoError(t, journal.Prepare(ctx, binding))
				require.Error(t, journal.Prepare(ctx, binding))
				var winners, physicalCalls atomic.Int32
				upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					physicalCalls.Add(1)
					_, _ = io.WriteString(w, "synthetic")
				}))
				defer upstream.Close()
				gate := make(chan struct{})
				var workers sync.WaitGroup
				for range 2 {
					workers.Go(func() {
						<-gate
						guard, e := relaycommon.NewDurableSingleAttempt(func() error { return journal.Dispatch(ctx, binding, 4, "synthetic-model") })
						if e != nil {
							return
						}
						info := &relaycommon.RelayInfo{ChannelMeta: &relaycommon.ChannelMeta{}, SingleAttempt: guard}
						c, _ := gin.CreateTestContext(httptest.NewRecorder())
						c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
						req, e := http.NewRequestWithContext(ctx, http.MethodPost, upstream.URL, bytes.NewReader([]byte("synthetic")))
						if e != nil {
							return
						}
						resp, e := channel.DoRequest(c, req, info)
						if e == nil {
							_, _ = io.Copy(io.Discard, resp.Body)
							_ = resp.Body.Close()
							winners.Add(1)
						}
					})
				}
				close(gate)
				workers.Wait()
				require.EqualValues(t, 1, winners.Load())
				require.EqualValues(t, 1, physicalCalls.Load())

				// Reopen and rerun the actual startup migration. The dispatch fence and
				// old-schema data must survive, even when an earlier process never completed.
				sqlDB, err := model.DB.DB()
				require.NoError(t, err)
				require.NoError(t, sqlDB.Close())
				require.NoError(t, model.InitDB())
				journal, err = model.NewGovernanceJournal(model.DB)
				require.NoError(t, err)
				require.ErrorIs(t, journal.Dispatch(ctx, binding, 4, "synthetic-model"), model.ErrGovernanceConflict)
				if upgrade {
					var sentinel model.Option
					require.NoError(t, model.DB.Where(&model.Option{Key: "governance_upgrade_sentinel"}).First(&sentinel).Error)
					assert.Equal(t, "preserve-this-value", sentinel.Value)
					var manifestOption model.Option
					require.NoError(t, model.DB.Where(&model.Option{Key: "governance_upgrade_manifest"}).First(&manifestOption).Error)
					var manifest struct {
						UserID  int    `json:"user_id"`
						TokenID int    `json:"token_id"`
						Digest  string `json:"key_digest"`
					}
					require.NoError(t, common.UnmarshalJsonStr(manifestOption.Value, &manifest))
					var user model.User
					var token model.Token
					require.NoError(t, model.DB.First(&user, manifest.UserID).Error)
					require.NoError(t, model.DB.First(&token, manifest.TokenID).Error)
					assert.Equal(t, "gov-upgrade", user.Username)
					assert.Equal(t, 54321, user.Quota)
					assert.Equal(t, common.UserStatusDisabled, user.Status)
					assert.Equal(t, user.Id, token.UserId)
					assert.Equal(t, 12345, token.RemainQuota)
					assert.Equal(t, 321, token.UsedQuota)
					digest := sha256.Sum256([]byte(token.Key))
					assert.Equal(t, manifest.Digest, hex.EncodeToString(digest[:]))
					require.Error(t, model.DB.Create(&model.Token{UserId: user.Id, Key: token.Key, Status: common.TokenStatusDisabled}).Error, "token uniqueness must survive upgrade")
					require.Error(t, model.DB.Create(&model.User{Username: user.Username, Status: common.UserStatusDisabled}).Error, "user uniqueness must survive upgrade")

				}
				require.ErrorIs(t, journal.Cancel(ctx, binding), model.ErrGovernanceConflict, "dispatched cannot be refunded as no-dispatch")
				badOwner := binding
				badOwner.OwnerID++
				require.Error(t, journal.Complete(ctx, badOwner, 2, 3, 5))
				require.Error(t, journal.Complete(ctx, binding, -1, 3, 2))
				require.Error(t, journal.Complete(ctx, binding, 2, 3, 9))
				require.Error(t, journal.Complete(ctx, binding, 1<<62, 0, 1<<62))
				// Failure to persist the outbox must roll back the state transition too.
				require.NoError(t, model.DB.Callback().Create().Before("gorm:create").Register("governance_outbox_failure", func(tx *gorm.DB) {
					if tx.Statement.Table == "governance_events" {
						tx.AddError(errors.New("synthetic outbox failure"))
					}
				}))
				require.Error(t, journal.CompleteDetailed(ctx, binding, 2, 3, 5, 1, 1, 0))
				require.NoError(t, model.DB.Callback().Create().Remove("governance_outbox_failure"))
				attempt, err := journal.Get(ctx, 7, binding.AttemptID)
				require.NoError(t, err)
				assert.Equal(t, model.GovernanceDispatched, attempt.State)
				require.NoError(t, journal.CompleteDetailed(ctx, binding, 2, 3, 5, 1, 1, 0))
				require.NoError(t, journal.CompleteDetailed(ctx, binding, 2, 3, 5, 1, 1, 0))
				require.ErrorIs(t, journal.Complete(ctx, binding, 3, 3, 6), model.ErrGovernanceConflict)
				events, err := journal.Pending(ctx, 7, 100)
				require.NoError(t, err)
				require.Len(t, events, 1)
				assert.EqualValues(t, 5, events[0].TotalTokens)
				require.Equal(t, 1, events[0].CacheDetailsVersion)
				require.EqualValues(t, 1, events[0].CacheReadTokens)
				require.ErrorIs(t, journal.CompleteDetailed(ctx, binding, 2, 3, 5, 1, 0, 1), model.ErrGovernanceConflict)
				require.ErrorIs(t, journal.CompleteDetailed(ctx, binding, 2, 3, 5, 1, 2, 1), model.ErrGovernanceInvalid)
				require.ErrorIs(t, journal.CompleteDetailed(ctx, binding, 2, 3, 5, 0, 1, 0), model.ErrGovernanceInvalid)
				other, err := journal.Pending(ctx, 8, 100)
				require.NoError(t, err)
				assert.Empty(t, other)
				require.Error(t, journal.Acknowledge(ctx, 8, binding.AttemptID))
				// Outbox rows survive another real restart, not just another handle.
				sqlDB, err = model.DB.DB()
				require.NoError(t, err)
				require.NoError(t, sqlDB.Close())
				require.NoError(t, model.InitDB())
				journal, err = model.NewGovernanceJournal(model.DB)
				require.NoError(t, err)
				events, err = journal.Pending(ctx, 7, 100)
				require.NoError(t, err)
				require.Len(t, events, 1)
				require.NoError(t, journal.Acknowledge(ctx, 7, binding.AttemptID))
				require.NoError(t, journal.Acknowledge(ctx, 7, binding.AttemptID))
				events, err = journal.Pending(ctx, 7, 100)
				require.NoError(t, err)
				assert.Empty(t, events)
				binding.AttemptID = strings.Repeat("d", 32)
				require.NoError(t, journal.Cancel(ctx, binding))
				require.NoError(t, journal.Cancel(ctx, binding))
				require.Error(t, journal.Prepare(ctx, binding), "cancel before admission must fence late requests")
				require.ErrorIs(t, journal.Dispatch(ctx, binding, 4, "synthetic-model"), model.ErrGovernanceConflict)
				events, err = journal.Pending(ctx, 7, 100)
				require.NoError(t, err)
				require.Len(t, events, 1)
				assert.Equal(t, model.GovernanceCancelled, events[0].State)
				require.Error(t, journal.Complete(ctx, binding, 0, 0, 0))
				binding.AttemptID = strings.Repeat("e", 32)
				require.NoError(t, journal.Prepare(ctx, binding))
				require.NoError(t, journal.Cancel(ctx, binding))
				attempt, err = journal.Get(ctx, 7, binding.AttemptID)
				require.NoError(t, err)
				assert.Equal(t, model.GovernanceCancelled, attempt.State)
				_, err = journal.Get(ctx, 8, binding.AttemptID)
				require.Error(t, err)

				racing := binding
				racing.AttemptID = strings.Repeat("f", 32)
				require.NoError(t, journal.Prepare(ctx, racing))
				start := make(chan struct{})
				cancelResult := make(chan error, 1)
				dispatchResult := make(chan error, 1)
				go func() { <-start; cancelResult <- journal.Cancel(ctx, racing) }()
				go func() { <-start; dispatchResult <- journal.Dispatch(ctx, racing, 4, "synthetic-model") }()
				close(start)
				cancelErr, dispatchErr := <-cancelResult, <-dispatchResult
				require.False(t, cancelErr == nil && dispatchErr == nil, "refund proof must never coexist with dispatch")
				final, err := journal.Get(ctx, 7, racing.AttemptID)
				require.NoError(t, err)
				if final.State == model.GovernanceCancelled {
					require.Error(t, dispatchErr)
				}
				if final.State == model.GovernanceDispatched {
					require.Error(t, cancelErr)
				}

				ttl := binding
				ttl.OwnerID = 93
				ttl.AttemptID = "TTLMatrixAttempt1234"
				require.NoError(t, journal.Prepare(ctx, ttl))
				require.NoError(t, journal.Dispatch(ctx, ttl, 4, "synthetic-model"))
				require.ErrorIs(t, journal.CompleteMetered(ctx, ttl, 11, 3, 14, 2, 4, 5, 3, 3), model.ErrGovernanceInvalid)
				require.ErrorIs(t, journal.CompleteMetered(ctx, ttl, 11, 3, 14, 1, 4, 5, 3, 2), model.ErrGovernanceInvalid)
				require.NoError(t, journal.CompleteMetered(ctx, ttl, 11, 3, 14, 2, 4, 5, 3, 2))
				require.NoError(t, journal.CompleteMetered(ctx, ttl, 11, 3, 14, 2, 4, 5, 3, 2))
				require.ErrorIs(t, journal.CompleteMetered(ctx, ttl, 11, 3, 14, 2, 4, 5, 2, 3), model.ErrGovernanceConflict)
				require.NoError(t, model.DB.AutoMigrate(&model.GovernanceAttempt{}, &model.GovernanceEvent{})) // repeated migration preserves TTL facts
				journal, err = model.NewGovernanceJournal(model.DB)
				require.NoError(t, err)
				ttlRows, err := journal.Pending(ctx, 93, 100)
				require.NoError(t, err)
				require.Len(t, ttlRows, 1)
				require.Equal(t, 2, ttlRows[0].CacheDetailsVersion)
				require.EqualValues(t, 3, ttlRows[0].CacheWrite5mTokens)
				require.EqualValues(t, 2, ttlRows[0].CacheWrite1hTokens)
				// More than one page, identical timestamps, an acknowledged cursor
				// and owner isolation must all work on the real database engine.
				var pending []model.GovernanceEvent
				for i := 0; i < 205; i++ {
					pending = append(pending, model.GovernanceEvent{AttemptID: fmt.Sprintf("PageAttempt%020d", i), OwnerID: 91, RequestID: fmt.Sprintf("PageRequest%020d", i), State: model.GovernanceCancelled, CreatedAt: 1234})
				}
				require.NoError(t, model.DB.Create(&pending).Error)
				require.NoError(t, model.DB.Create(&model.GovernanceEvent{AttemptID: "zzzzOtherOwner12345", OwnerID: 92, RequestID: "OtherRequest123456", State: model.GovernanceCancelled, CreatedAt: 1234}).Error)
				page, err := journal.PendingAfter(ctx, 91, 100, "")
				require.NoError(t, err)
				require.Len(t, page, 100)
				require.NoError(t, journal.Acknowledge(ctx, 91, page[99].AttemptID))
				_, err = journal.PendingAfter(ctx, 92, 100, page[99].AttemptID)
				require.Error(t, err)
				next, err := journal.PendingAfter(ctx, 91, 100, page[99].AttemptID)
				require.NoError(t, err)
				require.Len(t, next, 100)
				require.Equal(t, pending[100].AttemptID, next[0].AttemptID)
				last, err := journal.PendingAfter(ctx, 91, 100, next[99].AttemptID)
				require.NoError(t, err)
				require.Len(t, last, 5)
				require.Equal(t, pending[200].AttemptID, last[0].AttemptID)
				retry, err := journal.PendingAfter(ctx, 91, 100, "")
				require.NoError(t, err)
				require.Equal(t, pending[0].AttemptID, retry[0].AttemptID)
				_, err = journal.Pending(ctx, 7, 0)
				require.ErrorIs(t, err, model.ErrGovernanceInvalid)
				sqlDB, err = model.DB.DB()
				require.NoError(t, err)
				require.NoError(t, sqlDB.Close())
				guard, err := relaycommon.NewDurableSingleAttempt(func() error { return journal.Dispatch(ctx, binding, 4, "synthetic-model") })
				require.NoError(t, err)
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
				req, err := http.NewRequest(http.MethodPost, upstream.URL, bytes.NewReader([]byte("synthetic")))
				require.NoError(t, err)
				_, err = channel.DoRequest(c, req, &relaycommon.RelayInfo{ChannelMeta: &relaycommon.ChannelMeta{}, SingleAttempt: guard})
				require.ErrorContains(t, err, "fence unavailable")
				assert.EqualValues(t, 1, physicalCalls.Load())

			})
		}
	}
}
