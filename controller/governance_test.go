package controller

import (
	"context"
	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/middleware"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/setting/ratio_setting"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/QuantumNous/new-api/model"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestGovernanceTransportAndPrincipal(t *testing.T) {
	for _, tc := range []struct {
		name, auth, cookie, remote string
		allow                      bool
		identity                   int
		want                       int
	}{
		{"loopback", "Bearer sk-synthetic", "", "127.0.0.1:1234", true, 7, 204},
		{"disabled plaintext", "Bearer sk-synthetic", "", "127.0.0.1:1234", false, 7, 403},
		{"external plaintext", "Bearer sk-synthetic", "", "192.0.2.1:1234", true, 7, 403},
		{"cookie", "Bearer sk-synthetic", "session=x", "127.0.0.1:1234", true, 7, 401},
		{"missing bearer", "", "", "127.0.0.1:1234", true, 7, 401},
		{"channel suffix", "Bearer sk-synthetic-1", "", "127.0.0.1:1234", true, 7, 401},
		{"wrong owner", "Bearer sk-synthetic", "", "127.0.0.1:1234", true, 8, 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := gin.New()
			r.Use(GovernanceTransport(tc.allow), func(c *gin.Context) { c.Set("id", tc.identity); c.Set("token_id", 1) }, GovernancePrincipal(7))
			r.POST("/", func(c *gin.Context) { c.Status(204) })
			req := httptest.NewRequest("POST", "/", nil)
			req.RemoteAddr = tc.remote
			req.Header.Set("Authorization", tc.auth)
			req.Header.Set("Cookie", tc.cookie)
			req.Header.Set("X-Forwarded-Proto", "https")
			out := httptest.NewRecorder()
			r.ServeHTTP(out, req)
			require.Equal(t, tc.want, out.Code)
		})
	}
}
func TestGovernanceAdmissionEvidenceAndReplay(t *testing.T) {
	db := modelManagementDB(t, "sqlite", "")
	require.NoError(t, db.AutoMigrate(&model.GovernanceAttempt{}, &model.GovernanceEvent{}))
	const valid = `{"choices":[],"usage":{"prompt_tokens":2,"completion_tokens":3,"total_tokens":5}}`
	for _, mode := range []string{"complete", "stream", "estimated", "no-dispatch", "incomplete", "oversize"} {
		t.Run(mode, func(t *testing.T) {
			calls := 0
			r := gin.New()
			r.Use(func(c *gin.Context) { c.Set("id", 7) })
			r.POST("/", GovernanceAdmission(), func(c *gin.Context) {
				calls++
				x, _ := c.Get(governanceContextKey)
				execution := x.(*governanceExecution)
				if mode == "no-dispatch" {
					c.Status(400)
					return
				}
				require.NoError(t, execution.journal.Dispatch(c.Request.Context(), execution.binding, 1, "synthetic"))
				execution.info = &relaycommon.RelayInfo{UpstreamUsageEvidence: &relaycommon.UpstreamUsageEvidence{PromptTokens: 2, CompletionTokens: 3, TotalTokens: 5, Complete: mode != "incomplete"}}
				execution.succeeded = true
				body := valid
				if mode == "estimated" {
					body = strings.ReplaceAll(valid, "prompt_tokens\":2", "prompt_tokens\":8")
				}
				if mode == "oversize" {
					body = strings.Repeat("x", governanceResponseLimit+1)
				}
				if mode == "stream" {
					c.Render(-1, common.CustomEvent{Data: "data: " + valid})
					c.Render(-1, common.CustomEvent{Data: "data: [DONE]"})
				} else {
					c.Data(200, "application/json", []byte(body))
				}
			})
			for iteration := range 2 {
				req := httptest.NewRequest("POST", "/", strings.NewReader(`{"model":"synthetic","messages":[],"max_tokens":8}`))
				req.Header.Set("X-Aigw-Channel-Id", "1")
				req.Header.Set("X-Aigw-Request-Id", "Request_1234567890")
				req.Header.Set("X-Aigw-Attempt-Id", mode+"_Attempt1234567890")
				out := httptest.NewRecorder()
				r.ServeHTTP(out, req)
				if iteration == 1 {
					require.Equal(t, 409, out.Code)
					continue
				}
				if mode == "complete" || mode == "stream" {
					require.Equal(t, 200, out.Code)
					require.Equal(t, "1", out.Header().Get("X-Aigw-Evidence-Version"))
					if mode == "complete" {
						require.Equal(t, valid, out.Body.String())
					} else {
						require.Contains(t, out.Body.String(), "[DONE]")
					}
				} else {
					require.Equal(t, 502, out.Code)
					require.Empty(t, out.Header().Get("X-Aigw-Evidence-Version"))
				}
			}
			require.Equal(t, 1, calls)
			journal, err := model.NewGovernanceJournal(db)
			require.NoError(t, err)
			attempt, err := journal.Get(context.Background(), 7, mode+"_Attempt1234567890")
			require.NoError(t, err)
			want := model.GovernanceDispatched
			if mode == "complete" || mode == "stream" {
				want = model.GovernanceCompleted
			}
			if mode == "no-dispatch" {
				want = model.GovernanceCancelled
			}
			require.Equal(t, want, attempt.State)
		})
	}
}
func TestGovernanceStreamBodyEvidence(t *testing.T) {
	evidence := &relaycommon.UpstreamUsageEvidence{PromptTokens: 2, CompletionTokens: 3, TotalTokens: 5, Complete: true}
	usage := "data: {\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":3,\"total_tokens\":5}}\n\n"
	done := "data: [DONE]\n\n"
	require.True(t, governanceBodyMatches([]byte(usage+done), "text/event-stream", evidence))
	for _, body := range []string{usage, done, usage + usage + done, usage + done + usage, usage + "data: {\"error\":{}}\n\n" + done} {
		require.False(t, governanceBodyMatches([]byte(body), "text/event-stream", evidence))
	}
}

func TestGovernanceNativeAuthenticatedRelay(t *testing.T) {
	db := modelManagementDB(t, "sqlite", "")
	common.IsMasterNode = true
	require.NoError(t, model.InitDB())
	db = model.DB
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	require.NoError(t, db.AutoMigrate(&model.Log{}))
	require.NoError(t, ratio_setting.UpdateModelRatioByJSONString(`{"governance-model":1}`))
	require.NoError(t, ratio_setting.UpdateGroupRatioByJSONString(`{"default":1}`))
	user := model.User{Username: "governance-service", Status: common.UserStatusEnabled, Group: "default", Quota: 1000000, Setting: `{"billing_preference":"wallet_only"}`}
	require.NoError(t, db.Create(&user).Error)
	token := model.Token{UserId: user.Id, Key: "syntheticgovernanceservice", Status: common.TokenStatusEnabled, ExpiredTime: -1, RemainQuota: 1000000}
	require.NoError(t, db.Create(&token).Error)
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"synthetic","object":"chat.completion","model":"governance-model","choices":[{"index":0,"message":{"role":"assistant","content":"synthetic"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":3,"total_tokens":5}}`))
	}))
	defer upstream.Close()
	channel := model.Channel{Name: "governance-synthetic", Type: constant.ChannelTypeOpenAI, Key: "syntheticvendor", Status: common.ChannelStatusEnabled, Group: "default", Models: "governance-model", BaseURL: &upstream.URL}
	require.NoError(t, db.Create(&channel).Error)
	require.NoError(t, db.Create(&model.Ability{ChannelId: channel.Id, Model: "governance-model", Group: "default", Enabled: true}).Error)
	t.Setenv("GOVERNANCE_SERVICE_USER_ID", strconv.Itoa(user.Id))
	engine := gin.New()
	engine.Use(middleware.BodyStorageCleanup())
	engine.POST("/v1/chat/completions", middleware.TokenAuth(), func(c *gin.Context) { c.Status(204) })
	engine.POST("/internal/governance/v1/chat/completions", GovernanceTransport(true), middleware.TokenAuth(), GovernancePrincipal(user.Id), GovernanceAdmission(), middleware.Distribute(), func(c *gin.Context) { Relay(c, types.RelayFormatOpenAI) })
	call := func(key, attempt string, channelOverride ...string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("POST", "/internal/governance/v1/chat/completions", strings.NewReader(`{"model":"governance-model","messages":[{"role":"user","content":"synthetic"}],"max_tokens":8}`))
		req.RemoteAddr = "127.0.0.1:1234"
		req.Header.Set("Authorization", "Bearer sk-"+key)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Aigw-Channel-Id", "1")
		if len(channelOverride) > 0 {
			req.Header.Set("X-Aigw-Channel-Id", channelOverride[0])
		}
		req.Header.Set("X-Aigw-Request-Id", "Request_1234567890")
		req.Header.Set("X-Aigw-Attempt-Id", attempt)
		out := httptest.NewRecorder()
		engine.ServeHTTP(out, req)
		return out
	}

	bypass := httptest.NewRequest("POST", "/v1/chat/completions", nil)
	bypass.Header.Set("Authorization", "Bearer sk-"+token.Key)
	bypassResult := httptest.NewRecorder()
	engine.ServeHTTP(bypassResult, bypass)
	require.Equal(t, 403, bypassResult.Code)
	rejected := call("invalid", "InvalidAttempt12345")
	require.Equal(t, 401, rejected.Code)
	require.Zero(t, calls.Load())
	out := call(token.Key, "NativeAttempt12345")
	require.Equal(t, 200, out.Code, out.Body.String())
	require.Equal(t, "1", out.Header().Get("X-Aigw-Evidence-Version"))
	require.Equal(t, int32(1), calls.Load())
	duplicate := call(token.Key, "NativeAttempt12345")
	require.Equal(t, 409, duplicate.Code)
	require.Equal(t, int32(1), calls.Load())
	journal, err := model.NewGovernanceJournal(db)
	require.NoError(t, err)
	events, err := journal.Pending(context.Background(), user.Id, 100)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, int64(5), events[0].TotalTokens)

	mismatch := call(token.Key, "MismatchedAttempt12345", "2")
	require.Equal(t, 502, mismatch.Code)
	require.Equal(t, int32(1), calls.Load())
	mismatched, err := journal.Get(context.Background(), user.Id, "MismatchedAttempt12345")
	require.NoError(t, err)
	require.Equal(t, model.GovernanceCancelled, mismatched.State)
	require.NoError(t, db.Model(&channel).Update("type", constant.ChannelTypeAnthropic).Error)
	unsupported := call(token.Key, "UnsupportedAttempt12345")
	require.Equal(t, 502, unsupported.Code)
	require.Equal(t, int32(1), calls.Load())
	cancelled, err := journal.Get(context.Background(), user.Id, "UnsupportedAttempt12345")
	require.NoError(t, err)
	require.Equal(t, model.GovernanceCancelled, cancelled.State)
	require.NoError(t, db.Model(&token).Update("status", common.TokenStatusDisabled).Error)
	rejected = call(token.Key, "RevokedAttempt12345")
	require.Equal(t, 401, rejected.Code)
	require.Equal(t, int32(1), calls.Load())
}
