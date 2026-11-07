package controller

import (
	"bufio"
	"context"
	"encoding/json"
	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/i18n"
	"github.com/QuantumNous/new-api/middleware"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/setting/ratio_setting"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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
	for _, channelType := range []int{constant.ChannelTypeOpenAI, constant.ChannelTypeDeepSeek, constant.ChannelTypeAnthropic} {
		t.Run(strconv.Itoa(channelType), func(t *testing.T) { testGovernanceNativeAuthenticatedRelay(t, channelType) })
	}
}
func testGovernanceNativeAuthenticatedRelay(t *testing.T, channelType int) {
	require.NoError(t, i18n.Init())
	previousTimeout := constant.StreamingTimeout
	constant.StreamingTimeout = 30
	t.Cleanup(func() { constant.StreamingTimeout = previousTimeout })
	db := modelManagementDB(t, "sqlite", "")
	common.IsMasterNode = true
	require.NoError(t, model.InitDB())
	db = model.DB
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlDB.Close()) })
	require.NoError(t, db.AutoMigrate(&model.Log{}))
	originalModelRatios := ratio_setting.ModelRatio2JSONString()
	originalGroupRatios := ratio_setting.GroupRatio2JSONString()
	t.Cleanup(func() {
		require.NoError(t, ratio_setting.UpdateModelRatioByJSONString(originalModelRatios))
		require.NoError(t, ratio_setting.UpdateGroupRatioByJSONString(originalGroupRatios))
	})
	require.NoError(t, ratio_setting.UpdateModelRatioByJSONString(`{"governance-model":1}`))
	require.NoError(t, ratio_setting.UpdateGroupRatioByJSONString(`{"default":1}`))
	user := model.User{Username: "governance-service", Status: common.UserStatusEnabled, Group: "default", Quota: 1000000, Setting: `{"billing_preference":"wallet_only"}`}
	require.NoError(t, db.Create(&user).Error)
	token := model.Token{UserId: user.Id, Key: "syntheticgovernanceservice", Status: common.TokenStatusEnabled, ExpiredTime: -1, RemainQuota: 1000000}
	require.NoError(t, db.Create(&token).Error)
	var calls atomic.Int32
	var omitCache atomic.Bool
	var earlyInput bool
	var includeTTL atomic.Bool
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		var sent struct {
			MaxTokens uint `json:"max_tokens"`
			Stream    bool `json:"stream"`
		}
		require.NoError(t, common.DecodeJson(r.Body, &sent))
		require.Equal(t, uint(8), sent.MaxTokens)
		require.NotContains(t, r.Header.Get("Authorization"), token.Key)
		if channelType == constant.ChannelTypeAnthropic {
			require.Equal(t, "/v1/messages", r.URL.Path)
			require.Equal(t, "syntheticvendor", r.Header.Get("x-api-key"))
			if sent.Stream {
				w.Header().Set("Content-Type", "text/event-stream")
				for _, frame := range []string{
					`{"type":"message_start","message":{"id":"msg_synthetic","type":"message","role":"assistant","model":"governance-model","content":[],"usage":{"input_tokens":2,"output_tokens":0,"cache_read_input_tokens":4,"cache_creation_input_tokens":5}}}`,
					`{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}`,
					`{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"synthetic"}}`,
					`{"type":"content_block_stop","index":0}`,
					`{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":3}}`,
					`{"type":"message_stop"}`,
				} {
					_, _ = w.Write([]byte("data: " + frame + "\n\n"))
				}
				return
			}
			payload := `{"id":"synthetic","type":"message","role":"assistant","model":"governance-model","content":[{"type":"text","text":"synthetic"}],"stop_reason":"end_turn","usage":{"input_tokens":2,"output_tokens":3,"cache_read_input_tokens":4,"cache_creation_input_tokens":5}}`
			if includeTTL.Load() {
				payload = strings.Replace(payload, `"cache_creation_input_tokens":5`, `"cache_creation_input_tokens":5,"cache_creation":{"ephemeral_5m_input_tokens":3,"ephemeral_1h_input_tokens":2}`, 1)
			}
			_, _ = w.Write([]byte(payload))
			return
		}
		require.Equal(t, "Bearer syntheticvendor", r.Header.Get("Authorization"))
		payload := `{"id":"synthetic","object":"chat.completion","model":"governance-model","choices":[{"index":0,"message":{"role":"assistant","content":"synthetic"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":3,"total_tokens":5,"prompt_tokens_details":{"cached_tokens":1,"cached_creation_tokens":0}}}`
		if omitCache.Load() {
			payload = strings.ReplaceAll(payload, `,"prompt_tokens_details":{"cached_tokens":1,"cached_creation_tokens":0}`, "")
		}
		_, _ = w.Write([]byte(payload))
	}))
	defer upstream.Close()
	channel := model.Channel{Name: "governance-synthetic", Type: channelType, Key: "syntheticvendor", Status: common.ChannelStatusEnabled, Group: "default", Models: "governance-model", BaseURL: &upstream.URL}
	require.NoError(t, db.Create(&channel).Error)
	require.NoError(t, db.Create(&model.Ability{ChannelId: channel.Id, Model: "governance-model", Group: "default", Enabled: true}).Error)
	// An unreviewed route has higher priority for the same model. Governance
	// must deterministically select the reviewed channel instead of random routing.
	decoy := model.Channel{Name: "unreviewed-priority", Type: channelType, Key: "syntheticwrongvendor", Status: common.ChannelStatusEnabled, Group: "default", Models: "governance-model", BaseURL: &upstream.URL, Priority: common.GetPointer(int64(100))}
	require.NoError(t, db.Create(&decoy).Error)
	require.NoError(t, db.Create(&model.Ability{ChannelId: decoy.Id, Model: "governance-model", Group: "default", Enabled: true, Priority: common.GetPointer(int64(100))}).Error)
	t.Setenv("GOVERNANCE_SERVICE_USER_ID", strconv.Itoa(user.Id))
	engine := gin.New()
	engine.Use(middleware.BodyStorageCleanup())
	engine.POST("/v1/chat/completions", middleware.TokenAuth(), func(c *gin.Context) { c.Status(204) })
	engine.POST("/internal/governance/v1/chat/completions", GovernanceTransport(true), middleware.TokenAuth(), GovernancePrincipal(user.Id), GovernanceAdmission(), middleware.Distribute(), func(c *gin.Context) { Relay(c, types.RelayFormatOpenAI) })
	engine.GET("/internal/governance/v1/channels/:id/revision", GovernanceTransport(true), middleware.TokenAuth(), GovernancePrincipal(user.Id), GovernanceChannelConfiguration)
	tariff := true
	tariffVersion := "1"
	reviewedRevision := ""
	for _, key := range []string{"invalid", token.Key} {
		manifestRequest := httptest.NewRequest("GET", "/internal/governance/v1/channels/1/revision", nil)
		manifestRequest.RemoteAddr = "127.0.0.1:1234"
		manifestRequest.Header.Set("Authorization", "Bearer sk-"+key)
		result := httptest.NewRecorder()
		engine.ServeHTTP(result, manifestRequest)
		if key == "invalid" {
			require.Equal(t, 401, result.Code)
			continue
		}
		require.Equal(t, 200, result.Code)
		require.Equal(t, "no-store", result.Header().Get("Cache-Control"))
		require.NotContains(t, result.Body.String(), "syntheticvendor")
		require.NotContains(t, result.Body.String(), upstream.URL)
		var value struct {
			Revision string `json:"revision"`
		}
		require.NoError(t, common.Unmarshal(result.Body.Bytes(), &value))
		require.Len(t, value.Revision, 64)
		reviewedRevision = value.Revision
	}
	call := func(key, attempt string, channelOverride ...string) *httptest.ResponseRecorder {
		body := `{"model":"governance-model","messages":[{"role":"user","content":"synthetic"}],"max_tokens":8}`
		if earlyInput {
			body = strings.TrimSuffix(body, "}") + `,"stream":true}`
		}
		req := httptest.NewRequest("POST", "/internal/governance/v1/chat/completions", strings.NewReader(body))
		if earlyInput {
			req.Header.Set("X-Aigw-Admission-Version", "2")
			req.Header.Set("X-Aigw-Max-Input-Tokens", "128")
			req.Header.Set("X-Aigw-Stream-Version", "2")
			req.Header.Set("X-Aigw-Input-Evidence-Version", "1")
		}
		req.RemoteAddr = "127.0.0.1:1234"
		req.Header.Set("Authorization", "Bearer sk-"+key)
		req.Header.Set("Content-Type", "application/json")
		if tariff {
			req.Header.Set("X-Aigw-Tariff-Version", tariffVersion)
		}
		req.Header.Set("X-Aigw-Channel-Id", "1")
		if reviewedRevision != "" {
			req.Header.Set("X-Aigw-Config-Version", "1")
			req.Header.Set("X-Aigw-Channel-Revision", reviewedRevision)
		}
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
	require.NoError(t, db.Model(&token).Updates(map[string]any{"model_limits_enabled": true, "model_limits": "different-model"}).Error)
	require.Equal(t, 502, call(token.Key, "ModelDeniedAttempt12345").Code)
	require.Zero(t, calls.Load(), "governance pin bypassed token model limits")
	require.NoError(t, db.Model(&token).Update("model_limits_enabled", false).Error)
	require.NoError(t, db.Model(&model.Ability{}).Where("channel_id = ?", channel.Id).Update("enabled", false).Error)
	require.Equal(t, 502, call(token.Key, "ScopeDeniedAttempt12345").Code)
	require.Zero(t, calls.Load(), "governance pin bypassed channel group/model scope")
	require.NoError(t, db.Model(&model.Ability{}).Where("channel_id = ?", channel.Id).Update("enabled", true).Error)
	out := call(token.Key, "NativeAttempt12345")
	require.Equal(t, 200, out.Code, out.Body.String())
	require.Equal(t, "1", out.Header().Get("X-Aigw-Evidence-Version"))
	require.Equal(t, int32(1), calls.Load())
	require.Equal(t, reviewedRevision, out.Header().Get("X-Aigw-Channel-Revision"))
	require.Equal(t, "1", out.Header().Get("X-Aigw-Tariff-Version"))
	require.Contains(t, out.Body.String(), "cached_creation_tokens")
	duplicate := call(token.Key, "NativeAttempt12345")
	require.Equal(t, 409, duplicate.Code)
	require.Equal(t, int32(1), calls.Load())
	reviewedRevision = ""
	tariff = false
	journal, err := model.NewGovernanceJournal(db)
	require.NoError(t, err)
	for _, id := range []string{"ModelDeniedAttempt12345", "ScopeDeniedAttempt12345"} {
		attempt, err := journal.Get(context.Background(), user.Id, id)
		require.NoError(t, err)
		require.Equal(t, model.GovernanceCancelled, attempt.State)
		require.NoError(t, journal.Acknowledge(context.Background(), user.Id, id))
	}
	events, err := journal.Pending(context.Background(), user.Id, 100)
	require.NoError(t, err)
	require.Len(t, events, 1)
	if channelType == constant.ChannelTypeAnthropic {
		require.Equal(t, int64(11), events[0].PromptTokens)
		require.Equal(t, int64(14), events[0].TotalTokens)
	} else {
		require.Equal(t, int64(5), events[0].TotalTokens)
	}

	if channelType == constant.ChannelTypeDeepSeek {
		payload := `{"model":"governance-model","max_tokens":8,"messages":[{"role":"user","content":[{"type":"image_url","image_url":{"url":"data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4z8AAAAMBAQDJ/pLvAAAAAElFTkSuQmCC"}}]}]}`
		request := httptest.NewRequest("POST", "/internal/governance/v1/chat/completions", strings.NewReader(payload))
		request.RemoteAddr = "127.0.0.1:12345"
		request.Header.Set("Authorization", "Bearer sk-"+token.Key)
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-Aigw-Channel-Id", "1")
		request.Header.Set("X-Aigw-Request-Id", "ImageRequest1234567")
		request.Header.Set("X-Aigw-Attempt-Id", "ImageAttempt1234567")
		response := httptest.NewRecorder()
		engine.ServeHTTP(response, request)
		require.Equal(t, 502, response.Code)
		require.Equal(t, int32(1), calls.Load(), "unsupported vision channel contacted supplier")
	}
	inputRequest := httptest.NewRequest("POST", "/internal/governance/v1/chat/completions", strings.NewReader(`{"model":"governance-model","messages":[{"role":"user","content":"synthetic"}],"max_tokens":8}`))
	inputRequest.RemoteAddr = "127.0.0.1:12345"
	for key, value := range map[string]string{"Authorization": "Bearer sk-" + token.Key, "Content-Type": "application/json", "X-Aigw-Channel-Id": "1", "X-Aigw-Request-Id": "InputRequest1234567", "X-Aigw-Attempt-Id": "InputAttempt1234567", "X-Aigw-Admission-Version": "2", "X-Aigw-Max-Input-Tokens": "1"} {
		inputRequest.Header.Set(key, value)
	}
	inputResponse := httptest.NewRecorder()
	engine.ServeHTTP(inputResponse, inputRequest)
	require.Equal(t, 502, inputResponse.Code)
	require.Equal(t, int32(1), calls.Load(), "input ceiling denial contacted supplier")
	inputAttempt, err := journal.Get(context.Background(), user.Id, "InputAttempt1234567")
	require.NoError(t, err)
	require.Equal(t, model.GovernanceCancelled, inputAttempt.State)

	mismatch := call(token.Key, "MismatchedAttempt12345", "99999")
	require.Equal(t, 502, mismatch.Code)
	require.Equal(t, int32(1), calls.Load())
	mismatched, err := journal.Get(context.Background(), user.Id, "MismatchedAttempt12345")
	require.NoError(t, err)
	require.Equal(t, model.GovernanceCancelled, mismatched.State)
	for _, tc := range []struct{ field, value string }{
		{"model_mapping", `{"governance-model":"other-model"}`},
		{"setting", `{"tls_insecure_skip_verify":true}`},
		{"param_override", `{"max_tokens":100000}`},
		{"header_override", `{"Authorization":"{incoming_header:Authorization}"}`},
		{"setting", `{"system_prompt":"unreviewed content"}`},
		{"setting", `{"pass_through_body_enabled":true}`},
		{"settings", `{"advanced_custom":{}}`},
	} {
		t.Run(tc.field+tc.value, func(t *testing.T) {
			require.NoError(t, db.Model(&channel).Update(tc.field, tc.value).Error)
			attempt := "Transform" + common.GetUUID()
			result := call(token.Key, attempt)
			require.Equal(t, 502, result.Code)
			require.Equal(t, int32(1), calls.Load())
			cancelled, err := journal.Get(context.Background(), user.Id, attempt)
			require.NoError(t, err)
			require.Equal(t, model.GovernanceCancelled, cancelled.State)
			require.NoError(t, db.Model(&channel).Update(tc.field, "").Error)
		})
	}
	// A valid review cannot authorize a different endpoint or credential.
	configured, err := model.GetChannelById(channel.Id, true)
	require.NoError(t, err)
	reviewedRevision, err = model.GovernanceChannelRevision(configured)
	require.NoError(t, err)

	require.NoError(t, db.Model(&channel).Update("channel_info", `{"is_multi_key":true,"multi_key_size":1}`).Error)
	multi, err := model.GetChannelById(channel.Id, true)
	require.NoError(t, err)
	multiRevision, err := model.GovernanceChannelRevision(multi)
	require.NoError(t, err)
	savedRevision := reviewedRevision
	reviewedRevision = multiRevision
	deniedMulti := call(token.Key, "MultiKey"+common.GetUUID())
	require.Equal(t, 502, deniedMulti.Code)
	require.Equal(t, int32(1), calls.Load())
	require.NoError(t, db.Model(&channel).Update("channel_info", `{}`).Error)
	reviewedRevision = savedRevision
	for _, field := range []string{"key", "base_url"} {
		original := configured.Key
		changed := "synthetic-rotated"
		if field == "base_url" {
			original = *configured.BaseURL
			changed = upstream.URL + "/unreviewed"
		}
		require.NoError(t, db.Model(&channel).Update(field, changed).Error)
		attempt := "ConfigDrift" + common.GetUUID()
		denied := call(token.Key, attempt)
		require.Equal(t, 502, denied.Code, denied.Body.String())
		require.Equal(t, int32(1), calls.Load())
		row, err := journal.Get(context.Background(), user.Id, attempt)
		require.NoError(t, err)
		require.Equal(t, model.GovernanceCancelled, row.State)
		require.NoError(t, db.Model(&channel).Update(field, original).Error)
	}
	reviewedRevision = ""
	require.NoError(t, db.Model(&channel).Update("type", constant.ChannelTypeGemini).Error)
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
	if channelType != constant.ChannelTypeAnthropic {
		require.NoError(t, db.Model(&channel).Update("type", channelType).Error)
		require.NoError(t, db.Model(&token).Update("status", common.TokenStatusEnabled).Error)
		omitCache.Store(true)
		tariff = true
		attempt := "MissingCache" + common.GetUUID()
		response := call(token.Key, attempt)
		require.Equal(t, 502, response.Code)
		require.Equal(t, int32(2), calls.Load())
		row, err := journal.Get(context.Background(), user.Id, attempt)
		require.NoError(t, err)
		require.Equal(t, model.GovernanceDispatched, row.State)
	}

	// Governed selection must skip disabled credentials without rewriting status.
	require.NoError(t, db.Model(&channel).Update("type", channelType).Error)
	require.NoError(t, db.Model(&token).Update("status", common.TokenStatusEnabled).Error)
	omitCache.Store(false)
	for _, mode := range []string{"random", "polling"} {
		info := `{"is_multi_key":true,"multi_key_size":2,"multi_key_mode":"` + mode + `","multi_key_status_list":{"0":2},"multi_key_polling_index":99}`
		require.NoError(t, db.Model(&channel).Updates(map[string]any{"key": "disabled-secret\nsyntheticvendor", "channel_info": info}).Error)
		multi, err := model.GetChannelById(channel.Id, true)
		require.NoError(t, err)
		reviewedRevision, err = model.GovernanceChannelRevision(multi)
		require.NoError(t, err)
		before := calls.Load()
		attempt := "MultiAllowed" + common.GetUUID()
		result := call(token.Key, attempt)
		require.Equal(t, 200, result.Code, result.Body.String())
		require.Equal(t, before+1, calls.Load())
		replay := call(token.Key, attempt)
		require.NotEqual(t, 200, replay.Code)
		require.Equal(t, before+1, calls.Load())
		persisted, err := model.GetChannelById(channel.Id, true)
		require.NoError(t, err)
		require.Equal(t, 99, persisted.ChannelInfo.MultiKeyPollingIndex)
		require.Equal(t, 2, persisted.ChannelInfo.MultiKeyStatusList[0])
		require.NoError(t, db.Model(&channel).Update("key", "disabled-secret\nrotated-secret").Error)
		denied := call(token.Key, "MultiDrift"+common.GetUUID())
		require.Equal(t, 502, denied.Code)
		require.Equal(t, before+1, calls.Load())
	}

	// TTL evidence is opt-in; a positive aggregate without a duration is unknown.
	require.NoError(t, db.Model(&channel).Update("key", "disabled-secret\nsyntheticvendor").Error)
	reviewed, err := model.GetChannelById(channel.Id, true)
	require.NoError(t, err)
	reviewedRevision, err = model.GovernanceChannelRevision(reviewed)
	require.NoError(t, err)
	tariff, tariffVersion = true, "2"
	if channelType == constant.ChannelTypeAnthropic {
		missingID := "MissingTTL" + common.GetUUID()
		before := calls.Load()
		missing := call(token.Key, missingID)
		require.Equal(t, 502, missing.Code)
		require.Equal(t, before+1, calls.Load())
		unknown, err := journal.Get(context.Background(), user.Id, missingID)
		require.NoError(t, err)
		require.Equal(t, model.GovernanceDispatched, unknown.State)
	}
	includeTTL.Store(true)
	ttlAttempt := "TTLAttempt" + common.GetUUID()
	ttlResult := call(token.Key, ttlAttempt)
	require.Equal(t, 200, ttlResult.Code, ttlResult.Body.String())
	require.Equal(t, "2", ttlResult.Header().Get("X-Aigw-Tariff-Version"))
	require.Contains(t, ttlResult.Body.String(), "cached_creation_tokens_details")
	ttlRow, err := journal.Get(context.Background(), user.Id, ttlAttempt)
	require.NoError(t, err)
	require.Equal(t, 2, ttlRow.CacheDetailsVersion)
	if channelType == constant.ChannelTypeAnthropic {
		require.EqualValues(t, 3, ttlRow.CacheWrite5mTokens)
		require.EqualValues(t, 2, ttlRow.CacheWrite1hTokens)
	} else {
		require.Zero(t, ttlRow.CacheWrite5mTokens)
		require.Zero(t, ttlRow.CacheWrite1hTokens)
	}
	tariffVersion = "1"
	legacy := call(token.Key, "LegacyTTL"+common.GetUUID())
	require.Equal(t, 200, legacy.Code)
	require.NotContains(t, legacy.Body.String(), "cached_creation_tokens_details")
	earlyInput = true
	beforeEarly := calls.Load()
	earlyID := "EarlyUsage" + common.GetUUID()
	result := call(token.Key, earlyID)
	row, err := journal.Get(context.Background(), user.Id, earlyID)
	require.NoError(t, err)
	if channelType == constant.ChannelTypeAnthropic {
		require.Equal(t, 200, result.Code, result.Body.String())
		require.Equal(t, "1", result.Header().Get("X-Aigw-Input-Evidence-Version"))
		require.JSONEq(t, `{"input_tokens":2,"output_tokens":0,"cache_read_input_tokens":4,"cache_creation_input_tokens":5}`, result.Header().Get("X-Aigw-Initial-Usage"))
		require.Contains(t, result.Body.String(), "[DONE]")
		require.Equal(t, model.GovernanceCompleted, row.State)
		require.Equal(t, beforeEarly+1, calls.Load())
	} else {
		require.Equal(t, 502, result.Code)
		require.Equal(t, beforeEarly, calls.Load())
		require.Equal(t, model.GovernanceCancelled, row.State)
	}

}

func TestGovernanceLiveStreamCommitsBeforeTerminal(t *testing.T) {
	db := modelManagementDB(t, "sqlite", "")
	require.NoError(t, db.AutoMigrate(&model.GovernanceAttempt{}, &model.GovernanceEvent{}))
	for _, scenario := range []int{0, 1, 2, 3, 4} {
		version := scenario
		early := scenario >= 3
		if early {
			version = 1
		}
		cache := version > 0
		for _, complete := range []bool{true, false} {
			t.Run(strconv.Itoa(scenario)+"/"+strconv.FormatBool(complete), func(t *testing.T) {
				release := make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				defer unblock()
				r := gin.New()
				r.Use(func(c *gin.Context) { c.Set("id", 7) })
				r.POST("/", GovernanceAdmission(), func(c *gin.Context) {
					value, _ := c.Get(governanceContextKey)
					x := value.(*governanceExecution)
					require.NoError(t, x.journal.Dispatch(c.Request.Context(), x.binding, 1, "synthetic"))
					x.info = &relaycommon.RelayInfo{UpstreamInitialUsage: &relaycommon.UpstreamInitialUsage{InputTokens: 1, CacheReadTokens: 1}}
					if scenario == 4 {
						x.info.UpstreamInitialUsage.InputTokens = 2
					}
					c.Header("Content-Type", "text/event-stream")
					_, _ = c.Writer.WriteString("data: {\"choices\":[]}\n\n")
					c.Writer.Flush()
					<-release
					x.info.UpstreamUsageEvidence = &relaycommon.UpstreamUsageEvidence{CacheDetailsVersion: 1, CacheWriteTTLKnown: true, CacheReadTokens: 1, PromptTokens: 2, CompletionTokens: 3, TotalTokens: 5, Complete: complete}
					x.succeeded = true
					_, _ = c.Writer.WriteString("data: {\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":3,\"total_tokens\":5}}\n\ndata: [DONE]\n\n")
				})
				server := httptest.NewServer(r)
				defer func() { unblock(); server.Close() }()
				attempt := "LiveStream" + common.GetUUID()
				req, err := http.NewRequest("POST", server.URL, strings.NewReader(`{"model":"synthetic","messages":[],"max_tokens":8,"stream":true}`))
				require.NoError(t, err)
				req.Header.Set("X-Aigw-Stream-Version", "2")
				if early {
					req.Header.Set("X-Aigw-Input-Evidence-Version", "1")
					req.Header.Set("X-Aigw-Admission-Version", "2")
					req.Header.Set("X-Aigw-Max-Input-Tokens", "128")
					req.Header.Set("X-Aigw-Config-Version", "1")
					req.Header.Set("X-Aigw-Channel-Revision", strings.Repeat("a", 64))
				}
				if cache {
					req.Header.Set("X-Aigw-Tariff-Version", strconv.Itoa(version))
				}
				req.Header.Set("X-Aigw-Channel-Id", "1")
				req.Header.Set("X-Aigw-Request-Id", "Request_1234567890")
				req.Header.Set("X-Aigw-Attempt-Id", attempt)
				client := &http.Client{Timeout: 3 * time.Second}
				res, err := client.Do(req)
				if err != nil {
					unblock()
					t.Fatal(err)
				}
				defer res.Body.Close()
				require.Equal(t, "2", res.Header.Get("X-Aigw-Evidence-Version"))
				if early {
					require.Equal(t, "1", res.Header.Get("X-Aigw-Input-Evidence-Version"))
					require.NotEmpty(t, res.Header.Get("X-Aigw-Initial-Usage"))
				}
				reader := bufio.NewReader(res.Body)
				first, err := reader.ReadString('\n')
				require.NoError(t, err)
				require.Contains(t, first, "choices")
				journal, err := model.NewGovernanceJournal(db)
				require.NoError(t, err)
				before, err := journal.Get(context.Background(), 7, attempt)
				require.NoError(t, err)
				require.Equal(t, model.GovernanceDispatched, before.State)
				unblock()
				tail, err := io.ReadAll(reader)
				require.NoError(t, err)
				after, err := journal.Get(context.Background(), 7, attempt)
				require.NoError(t, err)
				if complete && scenario != 4 {
					require.Equal(t, "completed", res.Trailer.Get("X-Aigw-Completion"))
					require.Contains(t, string(tail), "[DONE]")
					require.Equal(t, model.GovernanceCompleted, after.State)
					if cache {
						require.Contains(t, string(tail), `"cached_tokens":1`)
						require.Equal(t, version, after.CacheDetailsVersion)
						if version == 2 {
							require.Contains(t, string(tail), "cached_creation_tokens_details")
							require.Equal(t, "2", res.Header.Get("X-Aigw-Tariff-Version"))
						}
						require.EqualValues(t, 1, after.CacheReadTokens)
					}
				} else {
					require.Empty(t, res.Trailer.Get("X-Aigw-Completion"))
					require.NotContains(t, string(tail), "[DONE]")
					require.Equal(t, model.GovernanceDispatched, after.State)
				}
			})
		}
	}
}

func TestGovernanceClientToolAdmission(t *testing.T) {
	for _, tc := range []struct {
		body  string
		valid bool
	}{
		{`{}`, true},
		{`{"tools":[{"type":"function","function":{"name":"lookup","parameters":{"type":"object"}}}],"tool_choice":"auto"}`, true},
		{`{"tools":[{"type":"web_search"}]}`, false},
		{`{"tools":[{"type":"function","function":{"name":"lookup"}}]}`, false},
		{`{"tools":[]}`, false},
		{`{"tool_choice":"auto"}`, false},
		{`{"tools":[{"type":"function","function":{"name":"lookup","parameters":{}}},{"type":"function","function":{"name":"lookup","parameters":{}}}]}`, false},
	} {
		var body map[string]json.RawMessage
		require.NoError(t, common.Unmarshal([]byte(tc.body), &body))
		require.Equal(t, tc.valid, governanceToolsValid(body), tc.body)
	}
}

func TestGovernanceInlineImageAdmission(t *testing.T) {
	const pixel = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4z8AAAAMBAQDJ/pLvAAAAAElFTkSuQmCC"
	for _, tc := range []struct {
		source string
		valid  bool
	}{
		{pixel, true}, {"http://127.0.0.1/private", false}, {"https://example.invalid/image.png", false}, {strings.Replace(pixel, "image/png", "image/jpeg", 1), false}, {pixel + "!", false}, {"data:image/svg+xml;base64,PHN2Zz4=", false},
	} {
		messages, err := common.Marshal([]any{map[string]any{"role": "user", "content": []any{map[string]any{"type": "image_url", "image_url": map[string]any{"url": tc.source}}}}})
		require.NoError(t, err)
		images, valid := governanceInlineImages(map[string]json.RawMessage{"messages": messages})
		require.Equal(t, tc.valid, valid)
		require.Equal(t, tc.valid, images)
	}
}

func TestGovernanceInputAdmissionHeaders(t *testing.T) {
	for _, tc := range []struct{ versions, limits []string }{
		{nil, []string{"128"}}, {[]string{"2"}, nil}, {[]string{"1"}, []string{"128"}},
		{[]string{"2", "2"}, []string{"128"}}, {[]string{"2"}, []string{"128", "128"}},
		{[]string{"2"}, []string{"0"}}, {[]string{"2"}, []string{"0128"}},
		{[]string{"2"}, []string{"2000001"}}, {[]string{"2"}, []string{"18446744073709551615"}},
	} {
		r := gin.New()
		r.POST("/", GovernanceAdmission(), func(c *gin.Context) { t.Error("invalid input approval reached relay") })
		req := httptest.NewRequest("POST", "/", strings.NewReader(`{"model":"synthetic","messages":[],"max_tokens":8}`))
		req.Header.Set("X-Aigw-Channel-Id", "1")
		req.Header.Set("X-Aigw-Request-Id", "Request_1234567890")
		req.Header.Set("X-Aigw-Attempt-Id", "Attempt_1234567890")
		for _, v := range tc.versions {
			req.Header.Add("X-Aigw-Admission-Version", v)
		}
		for _, v := range tc.limits {
			req.Header.Add("X-Aigw-Max-Input-Tokens", v)
		}
		out := httptest.NewRecorder()
		r.ServeHTTP(out, req)
		require.Equal(t, http.StatusBadRequest, out.Code)
	}
}
