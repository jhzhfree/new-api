package controller

import (
	"bytes"
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"strconv"
	"testing"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/middleware"
	"github.com/QuantumNous/new-api/model"
	"github.com/QuantumNous/new-api/setting/ratio_setting"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"
)

func TestGovernanceChannelOrigin(t *testing.T) {
	t.Setenv("GOVERNANCE_ALLOW_LOOPBACK_HTTP", "false")
	t.Setenv("GOVERNANCE_CHANNEL_ALLOWED_ORIGINS", "https://reviewed.example")
	for _, value := range []string{"https://api.deepseek.com", "https://reviewed.example/v1"} {
		assert.True(t, GovernanceChannelOrigin(value))
	}
	for _, value := range []string{"http://api.deepseek.com", "https://evil.example", "https://api.deepseek.com.evil.example", "https://user:secret@api.deepseek.com", "https://api.deepseek.com?key=secret", "https://api.deepseek.com#fragment", "https://api.deepseek.com?", "http://127.0.0.1"} {
		assert.False(t, GovernanceChannelOrigin(value))
	}
}

func TestGovernanceChannelManagementDatabaseMatrix(t *testing.T) {
	for _, dialect := range []struct{ kind, env string }{{"sqlite", ""}, {"mysql", "TEST_MYSQL_DSN"}, {"postgres", "TEST_POSTGRES_DSN"}} {
		t.Run(dialect.kind, func(t *testing.T) {
			if dialect.env != "" && os.Getenv(dialect.env) == "" {
				t.Skip("isolated database not configured")
			}
			db := modelManagementDB(t, dialect.kind, os.Getenv(dialect.env))
			require.NoError(t, db.AutoMigrate(&model.GovernanceChannelChange{}, &model.Token{}))
			require.NoError(t, db.AutoMigrate(&model.GovernanceChannelChange{}))
			previousGroups := ratio_setting.GroupRatio2JSONString()
			t.Cleanup(func() { require.NoError(t, ratio_setting.UpdateGroupRatioByJSONString(previousGroups)) })
			require.NoError(t, ratio_setting.UpdateGroupRatioByJSONString(`{"default":1}`))
			user := model.User{Username: "channel-admin", Status: common.UserStatusEnabled, Group: "default"}
			require.NoError(t, db.Create(&user).Error)
			token := model.Token{UserId: user.Id, Key: "syntheticchanneladmin", Status: common.TokenStatusEnabled, ExpiredTime: -1, UnlimitedQuota: true}
			require.NoError(t, db.Create(&token).Error)
			t.Setenv("GOVERNANCE_SERVICE_USER_ID", "99999")
			t.Setenv("GOVERNANCE_CHANNEL_ADMIN_USER_ID", strconv.Itoa(user.Id))
			router := gin.New()
			group := router.Group("/internal/governance-admin/v1", GovernanceTransport(true), middleware.TokenAuth(), GovernancePrincipal(user.Id))
			group.GET("/channels", GovernanceManagedChannels)
			group.GET("/changes/:operation", GovernanceManagedChannels)
			group.POST("/channels", GovernanceManagedChannels)
			group.PUT("/channels/:id", GovernanceManagedChannels)
			router.POST("/v1/chat/completions", middleware.TokenAuth(), func(c *gin.Context) { c.Status(204) })
			router.POST("/internal/governance/v1/chat/completions", middleware.TokenAuth(), func(c *gin.Context) { c.Status(204) })
			call := func(method, path, key string, body any) *httptest.ResponseRecorder {
				raw, err := common.Marshal(body)
				require.NoError(t, err)
				req := httptest.NewRequest(method, path, bytes.NewReader(raw))
				req.RemoteAddr = "127.0.0.1:1234"
				req.Header.Set("Authorization", "Bearer sk-"+key)
				w := httptest.NewRecorder()
				router.ServeHTTP(w, req)
				return w
			}
			assert.Equal(t, 403, call("POST", "/v1/chat/completions", token.Key, nil).Code)
			assert.Equal(t, 403, call("POST", "/internal/governance/v1/chat/completions", token.Key, nil).Code)
			assert.Equal(t, 401, call("GET", "/internal/governance-admin/v1/channels", "invalid", nil).Code)
			secret := "synthetic-vendor-one\nsynthetic-vendor-two"
			in := model.GovernanceChannelWrite{OperationID: "channel-create-0001", ActorIssuer: "https://identity.example", ActorSubject: "operator", Name: "reviewed", Type: 43, BaseURL: "https://api.deepseek.com", Models: []string{"deepseek-chat"}, Enabled: true, APIKey: &secret}
			w := call("POST", "/internal/governance-admin/v1/channels", token.Key, in)
			require.Equal(t, 200, w.Code)
			require.NotContains(t, w.Body.String(), secret)
			var receipt model.GovernanceChannelChange
			require.NoError(t, common.Unmarshal(w.Body.Bytes(), &receipt))
			assert.Equal(t, 200, call("POST", "/internal/governance-admin/v1/channels", token.Key, in).Code)
			var count int64
			require.NoError(t, db.Model(&model.Channel{}).Count(&count).Error)
			assert.EqualValues(t, 1, count)
			in.Name = "conflicting replay"
			assert.Equal(t, 409, call("POST", "/internal/governance-admin/v1/channels", token.Key, in).Code)
			w = call("GET", "/internal/governance-admin/v1/channels", token.Key, nil)
			require.Equal(t, 200, w.Code)
			assert.Equal(t, "no-store", w.Header().Get("Cache-Control"))
			assert.NotContains(t, w.Body.String(), "synthetic-vendor")
			var page struct {
				Data []model.GovernanceChannelView `json:"data"`
			}
			require.NoError(t, common.Unmarshal(w.Body.Bytes(), &page))
			require.Len(t, page.Data, 1)
			assert.Equal(t, 2, page.Data[0].KeyCount)
			common.MemoryCacheEnabled = true
			model.InitChannelCache()
			assert.True(t, model.IsChannelEnabledForGroupModel("default", "deepseek-chat", receipt.ChannelID))
			in.OperationID = "channel-update-0002"
			in.ExpectedRevision = page.Data[0].EditRevision
			in.APIKey = nil
			in.Enabled = false
			path := "/internal/governance-admin/v1/channels/" + strconv.Itoa(receipt.ChannelID)
			w = call("PUT", path, token.Key, in)
			require.Equal(t, 200, w.Code)
			stored, err := model.GetChannelById(receipt.ChannelID, true)
			require.NoError(t, err)
			assert.Equal(t, secret, stored.Key)
			assert.Equal(t, common.ChannelStatusManuallyDisabled, stored.Status)
			var ability model.Ability
			require.NoError(t, db.First(&ability, "channel_id = ?", receipt.ChannelID).Error)
			assert.False(t, ability.Enabled)
			assert.False(t, model.IsChannelEnabledForGroupModel("default", "deepseek-chat", receipt.ChannelID), "channel edit left stale enabled cache")
			in.OperationID = "channel-update-stale"
			assert.Equal(t, 409, call("PUT", path, token.Key, in).Code)
			in.ExpectedRevision = model.GovernanceChannelPublic(stored).EditRevision
			in.Enabled = true
			require.NoError(t, db.Callback().Create().Before("gorm:create").Register("test:channel-audit-failure", func(tx *gorm.DB) {
				if tx.Statement.Table == "governance_channel_changes" {
					tx.AddError(errors.New("synthetic audit failure"))
				}
			}))
			assert.Equal(t, 503, call("PUT", path, token.Key, in).Code)
			require.NoError(t, db.Callback().Create().Remove("test:channel-audit-failure"))
			stored, err = model.GetChannelById(receipt.ChannelID, true)
			require.NoError(t, err)
			assert.Equal(t, common.ChannelStatusManuallyDisabled, stored.Status)
			_, err = model.GovernanceChannelReceipt(context.Background(), user.Id+1, receipt.ID)
			assert.ErrorIs(t, err, gorm.ErrRecordNotFound)
			assert.Equal(t, 200, call("GET", "/internal/governance-admin/v1/changes/"+receipt.ID, token.Key, nil).Code)
			require.NoError(t, db.Model(&token).Update("status", common.TokenStatusDisabled).Error)
			assert.Equal(t, 401, call("GET", "/internal/governance-admin/v1/channels", token.Key, nil).Code)
		})
	}
}
