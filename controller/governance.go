package controller

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/model"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/gin-gonic/gin"
)

const governanceContextKey = "internal_governance_execution"
const governanceResponseLimit = 4 << 20

var governanceIdentifier = regexp.MustCompile(`^[A-Za-z0-9_-]{16,64}$`)

type governanceExecution struct {
	binding         model.GovernanceBinding
	journal         *model.GovernanceJournal
	info            *relaycommon.RelayInfo
	succeeded       bool
	expectedChannel int
	maxTokens       uint
}

// GovernanceTransport runs before authentication. Forwarded headers cannot
// establish transport trust. Loopback plaintext is an explicit local test mode.
func GovernanceTransport(allowLoopback bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		host, _, _ := net.SplitHostPort(c.Request.RemoteAddr)
		if c.Request.TLS == nil && !(allowLoopback && net.ParseIP(host).IsLoopback()) {
			c.AbortWithStatus(http.StatusForbidden)
			return
		}
		values := c.Request.Header.Values("Authorization")
		if len(values) != 1 || !strings.HasPrefix(values[0], "Bearer sk-") || strings.Contains(strings.TrimPrefix(values[0], "Bearer sk-"), "-") || c.Request.URL.RawQuery != "" || c.GetHeader("Cookie") != "" || c.GetHeader("Sec-WebSocket-Protocol") != "" {
			c.AbortWithStatus(http.StatusUnauthorized)
			return
		}
		c.Next()
	}
}

// GovernancePrincipal must follow native TokenAuth. The configured user is a
// dedicated service identity; token expiry/revocation and user status stay native.
func GovernancePrincipal(owner int) gin.HandlerFunc {
	return func(c *gin.Context) {
		if owner <= 0 || c.GetInt("id") != owner || c.GetInt("token_id") <= 0 {
			c.AbortWithStatus(http.StatusForbidden)
			return
		}
		c.Next()
	}
}

// GovernanceAdmission supports buffered v1 and negotiated real-time SSE v2.
// V2 releases content early, but DONE and the completion trailer require a durable commit.
func GovernanceAdmission() gin.HandlerFunc {
	slots := make(chan struct{}, 32)
	return func(c *gin.Context) {
		select {
		case slots <- struct{}{}:
			defer func() { <-slots }()
		default:
			c.AbortWithStatus(http.StatusTooManyRequests)
			return
		}

		requestIDs, attemptIDs := c.Request.Header.Values("X-Aigw-Request-Id"), c.Request.Header.Values("X-Aigw-Attempt-Id")
		if len(requestIDs) != 1 || len(attemptIDs) != 1 || !governanceIdentifier.MatchString(requestIDs[0]) || !governanceIdentifier.MatchString(attemptIDs[0]) {
			c.AbortWithStatus(http.StatusBadRequest)
			return
		}
		raw, err := io.ReadAll(io.LimitReader(c.Request.Body, (1<<20)+1))
		_ = c.Request.Body.Close()
		if err != nil || len(raw) > 1<<20 {
			c.AbortWithStatus(http.StatusRequestEntityTooLarge)
			return
		}
		var payload map[string]json.RawMessage
		if common.Unmarshal(raw, &payload) != nil || len(payload) == 0 {
			c.AbortWithStatus(http.StatusBadRequest)
			return
		}
		for key := range payload {
			switch key {
			case "model", "messages", "max_tokens", "stream", "stream_options":
			default:
				c.AbortWithStatus(http.StatusBadRequest)
				return
			}
		}
		var maxTokens uint
		if common.Unmarshal(payload["max_tokens"], &maxTokens) != nil || maxTokens == 0 || maxTokens > 1000000 {
			c.AbortWithStatus(http.StatusBadRequest)
			return
		}
		var stream bool
		if value := payload["stream"]; len(value) > 0 && common.Unmarshal(value, &stream) != nil {
			c.AbortWithStatus(http.StatusBadRequest)
			return
		}
		streamV2 := stream && c.GetHeader("X-Aigw-Stream-Version") == "2"
		channelIDs := c.Request.Header.Values("X-Aigw-Channel-Id")
		if len(channelIDs) != 1 {
			c.AbortWithStatus(http.StatusBadRequest)
			return
		}
		channelID, err := strconv.Atoi(channelIDs[0])
		if err != nil || channelID <= 0 || strconv.Itoa(channelID) != channelIDs[0] {
			c.AbortWithStatus(http.StatusBadRequest)
			return
		}
		digest := sha256.Sum256(append([]byte(channelIDs[0]+"\x00"), raw...))
		journal, err := model.NewGovernanceJournal(model.DB)
		if err != nil {
			c.AbortWithStatus(http.StatusServiceUnavailable)
			return
		}
		execution := &governanceExecution{maxTokens: maxTokens, expectedChannel: channelID, journal: journal, binding: model.GovernanceBinding{OwnerID: c.GetInt("id"), RequestID: requestIDs[0], AttemptID: attemptIDs[0], Fingerprint: hex.EncodeToString(digest[:])}}
		if journal.Prepare(c.Request.Context(), execution.binding) != nil {
			c.AbortWithStatus(http.StatusConflict)
			return
		}
		c.Set(governanceContextKey, execution)
		c.Request.Body = io.NopCloser(bytes.NewReader(raw))
		c.Request.URL.Path = "/v1/chat/completions"
		original := c.Writer
		buffered := &governanceWriter{ResponseWriter: original, header: make(http.Header), status: 200, size: -1, live: streamV2, binding: execution.binding}
		c.Writer = buffered
		defer func() { c.Writer = original }()
		c.Next()
		c.Writer = original
		finish, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if execution.info == nil || !execution.succeeded || buffered.failed || buffered.status != 200 || !governanceBodyMatches(buffered.body.Bytes(), buffered.header.Get("Content-Type"), execution.info.UpstreamUsageEvidence) {
			// Cancellation only succeeds if dispatch never happened. Otherwise the
			// durable state stays unknown and the control plane must retain its hold.
			_ = journal.Cancel(finish, execution.binding)
			if buffered.started {
				_, _ = original.Write([]byte("data: {\"error\":{\"code\":\"governance_incomplete\"}}\n\n"))
				original.Flush()
			} else {
				c.AbortWithStatus(http.StatusBadGateway)
			}
			return
		}
		usage := execution.info.UpstreamUsageEvidence
		if journal.Complete(finish, execution.binding, int64(usage.PromptTokens), int64(usage.CompletionTokens), int64(usage.TotalTokens)) != nil {
			if buffered.started {
				_, _ = original.Write([]byte("data: {\"error\":{\"code\":\"governance_commit_failed\"}}\n\n"))
				original.Flush()
			} else {
				c.AbortWithStatus(http.StatusServiceUnavailable)
			}
			return
		}
		if buffered.started {
			original.Header().Set("X-Aigw-Completion", "completed")
			_, _ = original.Write([]byte("data: [DONE]\n\n"))
			original.Flush()
			return
		}
		original.Header().Set("Content-Type", buffered.header.Get("Content-Type"))
		original.Header().Set("Cache-Control", "no-store")
		for k, v := range map[string]string{"X-Aigw-Evidence-Version": "1", "X-Aigw-Request-Id": execution.binding.RequestID, "X-Aigw-Attempt-Id": execution.binding.AttemptID, "X-Aigw-Usage-Source": "upstream", "X-Aigw-Physical-Attempts": "1"} {
			original.Header().Set(k, v)
		}
		original.WriteHeader(200)
		_, _ = original.Write(buffered.body.Bytes())
	}
}

// governanceBodyMatches rejects normalized/estimated counters, duplicate usage,
// malformed frames, errors and bytes following DONE before issuing evidence.
func governanceBodyMatches(body []byte, contentType string, evidence *relaycommon.UpstreamUsageEvidence) bool {
	if evidence == nil || !evidence.Complete {
		return false
	}
	seen := false
	check := func(raw []byte) bool {
		var frame struct {
			Error json.RawMessage `json:"error"`
			Usage *struct {
				Prompt     *int `json:"prompt_tokens"`
				Completion *int `json:"completion_tokens"`
				Total      *int `json:"total_tokens"`
			} `json:"usage"`
		}
		if common.Unmarshal(raw, &frame) != nil || len(frame.Error) > 0 {
			return false
		}
		if frame.Usage == nil {
			return true
		}
		u := frame.Usage
		if seen || u.Prompt == nil || u.Completion == nil || u.Total == nil || *u.Prompt != evidence.PromptTokens || *u.Completion != evidence.CompletionTokens || *u.Total != evidence.TotalTokens {
			return false
		}
		seen = true
		return true
	}
	if strings.HasPrefix(contentType, "application/json") {
		return check(body) && seen
	}
	if !strings.HasPrefix(contentType, "text/event-stream") {
		return false
	}
	scanner := bufio.NewScanner(bytes.NewReader(body))
	scanner.Buffer(make([]byte, 4096), 64<<10)
	done := false
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" || strings.HasPrefix(line, ":") {
			continue
		}
		if done || !strings.HasPrefix(line, "data: ") {
			return false
		}
		data := strings.TrimPrefix(line, "data: ")
		if data == "[DONE]" {
			done = true
			continue
		}
		if !check([]byte(data)) {
			return false
		}
	}
	return scanner.Err() == nil && done && seen
}

type governanceWriter struct {
	gin.ResponseWriter
	header                  http.Header
	body                    bytes.Buffer
	status, size            int
	failed                  bool
	live, started, terminal bool
	pending                 []byte
	binding                 model.GovernanceBinding
}

func (w *governanceWriter) Header() http.Header { return w.header }
func (w *governanceWriter) WriteHeader(code int) {
	if code > 0 && w.size < 0 {
		w.status = code
	}
}
func (w *governanceWriter) WriteHeaderNow() {
	if w.size < 0 {
		w.size = 0
	}
}
func (w *governanceWriter) Write(p []byte) (int, error) {
	w.WriteHeaderNow()
	if w.failed || w.body.Len()+len(p) > governanceResponseLimit {
		w.failed = true
		return 0, errors.New("governance response limit")
	}
	n, err := w.body.Write(p)
	w.size += n
	if err == nil && w.live && w.status == 200 && strings.HasPrefix(w.header.Get("Content-Type"), "text/event-stream") {
		w.pending = append(w.pending, p...)
		for {
			index := bytes.IndexByte(w.pending, '\n')
			if index < 0 {
				break
			}
			line := w.pending[:index+1]
			w.pending = w.pending[index+1:]
			if strings.TrimSpace(string(line)) == "data: [DONE]" {
				w.terminal = true
				continue
			}
			if w.terminal {
				continue
			}
			if !w.started {
				w.ResponseWriter.Header().Set("Content-Type", "text/event-stream")
				w.ResponseWriter.Header().Set("Cache-Control", "no-store")
				w.ResponseWriter.Header().Set("Trailer", "X-Aigw-Completion")
				for key, value := range map[string]string{"X-Aigw-Evidence-Version": "2", "X-Aigw-Request-Id": w.binding.RequestID, "X-Aigw-Attempt-Id": w.binding.AttemptID, "X-Aigw-Usage-Source": "upstream", "X-Aigw-Physical-Attempts": "1"} {
					w.ResponseWriter.Header().Set(key, value)
				}
				w.ResponseWriter.WriteHeader(200)
				w.started = true
			}
			if _, err = w.ResponseWriter.Write(line); err != nil {
				w.failed = true
				return 0, err
			}
		}
	}
	return n, err
}
func (w *governanceWriter) WriteString(s string) (int, error) { return w.Write([]byte(s)) }
func (w *governanceWriter) Status() int                       { return w.status }
func (w *governanceWriter) Size() int                         { return w.size }
func (w *governanceWriter) Written() bool                     { return w.size >= 0 }
func (w *governanceWriter) Flush() {
	w.WriteHeaderNow()
	if w.started {
		w.ResponseWriter.Flush()
	}
}
func (w *governanceWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return nil, nil, errors.New("governance does not support hijacking")
}
func (w *governanceWriter) Pusher() http.Pusher { return nil }

func GovernanceEvents(c *gin.Context) {
	journal, err := model.NewGovernanceJournal(model.DB)
	if err != nil {
		c.AbortWithStatus(503)
		return
	}
	events, err := journal.Pending(c.Request.Context(), c.GetInt("id"), 100)
	if err != nil {
		c.AbortWithStatus(503)
		return
	}
	c.Header("Cache-Control", "no-store")
	c.Header("X-Aigw-Events-Version", "1")
	c.JSON(200, events)
}
func GovernanceAcknowledge(c *gin.Context) {
	journal, err := model.NewGovernanceJournal(model.DB)
	if err != nil || journal.Acknowledge(c.Request.Context(), c.GetInt("id"), c.Param("id")) != nil {
		c.AbortWithStatus(409)
		return
	}
	c.Status(204)
}

// GovernanceCancel atomically fences an absent/prepared attempt. A dispatched
// attempt cannot be declared unexecuted, even if its client has timed out.
func GovernanceCancel(c *gin.Context) {
	raw, err := io.ReadAll(io.LimitReader(c.Request.Body, 1025))
	if err != nil || len(raw) > 1024 {
		c.AbortWithStatus(400)
		return
	}
	var payload struct {
		RequestID   string `json:"request_id"`
		Fingerprint string `json:"fingerprint"`
	}
	if common.Unmarshal(raw, &payload) != nil {
		c.AbortWithStatus(400)
		return
	}
	journal, err := model.NewGovernanceJournal(model.DB)
	if err != nil {
		c.AbortWithStatus(503)
		return
	}
	binding := model.GovernanceBinding{OwnerID: c.GetInt("id"), RequestID: payload.RequestID, AttemptID: c.Param("id"), Fingerprint: payload.Fingerprint}
	if err := journal.Cancel(c.Request.Context(), binding); err != nil {
		if errors.Is(err, model.ErrGovernanceConflict) {
			c.AbortWithStatus(409)
		} else if errors.Is(err, model.ErrGovernanceInvalid) {
			c.AbortWithStatus(400)
		} else {
			c.AbortWithStatus(503)
		}
		return
	}
	c.Status(204)
}

func governanceChannelSupported(channelType int) bool {
	switch channelType {
	case constant.ChannelTypeOpenAI, constant.ChannelTypeDeepSeek, constant.ChannelTypeAnthropic:
		return true
	default:
		return false
	}
}
