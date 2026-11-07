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
	"strings"
	"time"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/model"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/gin-gonic/gin"
)

const governanceContextKey = "internal_governance_execution"
const governanceResponseLimit = 4 << 20

var governanceIdentifier = regexp.MustCompile(`^[A-Za-z0-9_-]{16,64}$`)

type governanceExecution struct {
	binding   model.GovernanceBinding
	journal   *model.GovernanceJournal
	info      *relaycommon.RelayInfo
	succeeded bool
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

// GovernanceAdmission buffers governed responses until durable completion is
// known. This bounded first version deliberately does not offer real-time SSE.
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
		digest := sha256.Sum256(raw)
		journal, err := model.NewGovernanceJournal(model.DB)
		if err != nil {
			c.AbortWithStatus(http.StatusServiceUnavailable)
			return
		}
		execution := &governanceExecution{journal: journal, binding: model.GovernanceBinding{OwnerID: c.GetInt("id"), RequestID: requestIDs[0], AttemptID: attemptIDs[0], Fingerprint: hex.EncodeToString(digest[:])}}
		if journal.Prepare(c.Request.Context(), execution.binding) != nil {
			c.AbortWithStatus(http.StatusConflict)
			return
		}
		c.Set(governanceContextKey, execution)
		c.Request.Body = io.NopCloser(bytes.NewReader(raw))
		c.Request.URL.Path = "/v1/chat/completions"
		original := c.Writer
		buffered := &governanceWriter{ResponseWriter: original, header: make(http.Header), status: 200, size: -1}
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
			c.AbortWithStatus(http.StatusBadGateway)
			return
		}
		usage := execution.info.UpstreamUsageEvidence
		if journal.Complete(finish, execution.binding, int64(usage.PromptTokens), int64(usage.CompletionTokens), int64(usage.TotalTokens)) != nil {
			c.AbortWithStatus(http.StatusServiceUnavailable)
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
	header       http.Header
	body         bytes.Buffer
	status, size int
	failed       bool
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
	return n, err
}
func (w *governanceWriter) WriteString(s string) (int, error) { return w.Write([]byte(s)) }
func (w *governanceWriter) Status() int                       { return w.status }
func (w *governanceWriter) Size() int                         { return w.size }
func (w *governanceWriter) Written() bool                     { return w.size >= 0 }
func (w *governanceWriter) Flush()                            { w.WriteHeaderNow() }
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
