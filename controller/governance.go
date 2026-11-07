package controller

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"image"
	_ "image/jpeg"
	_ "image/png"
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

var governanceRevision = regexp.MustCompile(`^[a-f0-9]{64}$`)

var governanceIdentifier = regexp.MustCompile(`^[A-Za-z0-9_-]{16,64}$`)

type governanceExecution struct {
	binding          model.GovernanceBinding
	journal          *model.GovernanceJournal
	info             *relaycommon.RelayInfo
	succeeded        bool
	expectedRevision string
	expectedChannel  int
	maxInputTokens   uint
	maxTokens        uint
	hasImages        bool
	embedding        bool
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
func GovernanceAdmission() gin.HandlerFunc          { return governanceAdmission(false) }
func GovernanceEmbeddingAdmission() gin.HandlerFunc { return governanceAdmission(true) }
func governanceAdmission(embedding bool) gin.HandlerFunc {
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
		var hasImages, stream bool
		var maxTokens uint
		if embedding {
			if _, err := relaycommon.GovernedEmbeddingEstimate(raw, ""); err != nil {
				c.AbortWithStatus(400)
				return
			}
		} else {
			for key := range payload {
				switch key {
				case "model", "messages", "max_tokens", "stream", "stream_options", "tools", "tool_choice":
				default:
					c.AbortWithStatus(http.StatusBadRequest)
					return
				}
			}
			var imagesValid bool
			hasImages, imagesValid = governanceInlineImages(payload)
			if !imagesValid || !governanceToolsValid(payload) {
				c.AbortWithStatus(http.StatusBadRequest)
				return
			}
			if common.Unmarshal(payload["max_tokens"], &maxTokens) != nil || maxTokens == 0 || maxTokens > 1000000 {
				c.AbortWithStatus(http.StatusBadRequest)
				return
			}
			if value := payload["stream"]; len(value) > 0 && common.Unmarshal(value, &stream) != nil {
				c.AbortWithStatus(http.StatusBadRequest)
				return
			}
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
		admissionVersions := c.Request.Header.Values("X-Aigw-Admission-Version")
		inputLimits := c.Request.Header.Values("X-Aigw-Max-Input-Tokens")
		inputLimit := ""
		var maxInput uint64
		fingerprintPrefix := channelIDs[0] + "\x00"
		if len(admissionVersions) > 0 || len(inputLimits) > 0 {
			if len(admissionVersions) != 1 || admissionVersions[0] != "2" || len(inputLimits) != 1 {
				c.AbortWithStatus(http.StatusBadRequest)
				return
			}
			inputLimit = inputLimits[0]
			maxInput, err = strconv.ParseUint(inputLimit, 10, 32)
			if err != nil || maxInput < 1 || maxInput > 2000000 || strconv.FormatUint(maxInput, 10) != inputLimit {
				c.AbortWithStatus(http.StatusBadRequest)
				return
			}
			fingerprintPrefix += "v2\x00" + inputLimit + "\x00"
		}
		if embedding {
			if inputLimit == "" {
				c.AbortWithStatus(400)
				return
			}
			fingerprintPrefix += "embeddings\x00"
		}
		expectedRevision := ""
		versions, revisions := c.Request.Header.Values("X-Aigw-Config-Version"), c.Request.Header.Values("X-Aigw-Channel-Revision")
		if len(versions) > 0 || len(revisions) > 0 {
			if len(versions) != 1 || versions[0] != "1" || len(revisions) != 1 || !governanceRevision.MatchString(revisions[0]) {
				c.AbortWithStatus(400)
				return
			}
			expectedRevision = revisions[0]
			common.SetContextKey(c, constant.ContextKeyGovernanceRevisionExpected, expectedRevision)
			fingerprintPrefix += "config-v1\x00" + expectedRevision + "\x00"
		}
		digest := sha256.Sum256(append([]byte(fingerprintPrefix), raw...))
		journal, err := model.NewGovernanceJournal(model.DB)
		if err != nil {
			c.AbortWithStatus(http.StatusServiceUnavailable)
			return
		}
		execution := &governanceExecution{expectedRevision: expectedRevision, embedding: embedding, maxInputTokens: uint(maxInput), hasImages: hasImages, maxTokens: maxTokens, expectedChannel: channelID, journal: journal, binding: model.GovernanceBinding{OwnerID: c.GetInt("id"), RequestID: requestIDs[0], AttemptID: attemptIDs[0], Fingerprint: hex.EncodeToString(digest[:])}}
		if journal.Prepare(c.Request.Context(), execution.binding) != nil {
			c.AbortWithStatus(http.StatusConflict)
			return
		}
		c.Set(governanceContextKey, execution)
		c.Request.Body = io.NopCloser(bytes.NewReader(raw))
		c.Request.URL.Path = "/v1/chat/completions"
		if embedding {
			c.Request.URL.Path = "/v1/embeddings"
		}
		original := c.Writer
		buffered := &governanceWriter{ResponseWriter: original, header: make(http.Header), status: 200, size: -1, inputLimit: inputLimit, revision: expectedRevision, live: streamV2, binding: execution.binding}
		c.Writer = buffered
		defer func() { c.Writer = original }()
		c.Next()
		c.Writer = original
		finish, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if execution.info == nil || !execution.succeeded || buffered.failed || buffered.status != 200 || !governanceBodyMatches(buffered.body.Bytes(), buffered.header.Get("Content-Type"), execution.info.UpstreamUsageEvidence, embedding) {
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
		if expectedRevision != "" {
			original.Header().Set("X-Aigw-Config-Version", "1")
			original.Header().Set("X-Aigw-Channel-Revision", expectedRevision)
		}
		if embedding {
			original.Header().Set("X-Aigw-Protocol", "embeddings")
		}
		if inputLimit != "" {
			original.Header().Set("X-Aigw-Admission-Version", "2")
			original.Header().Set("X-Aigw-Input-Limit", inputLimit)
		}
		for k, v := range map[string]string{"X-Aigw-Evidence-Version": "1", "X-Aigw-Request-Id": execution.binding.RequestID, "X-Aigw-Attempt-Id": execution.binding.AttemptID, "X-Aigw-Usage-Source": "upstream", "X-Aigw-Physical-Attempts": "1"} {
			original.Header().Set(k, v)
		}
		original.WriteHeader(200)
		_, _ = original.Write(buffered.body.Bytes())
	}
}

// governanceBodyMatches rejects normalized/estimated counters, duplicate usage,
// malformed frames, errors and bytes following DONE before issuing evidence.
func governanceBodyMatches(body []byte, contentType string, evidence *relaycommon.UpstreamUsageEvidence, embedding ...bool) bool {
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
		if len(embedding) > 0 && embedding[0] && frame.Usage.Completion == nil {
			zero := 0
			frame.Usage.Completion = &zero
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
	inputLimit              string
	revision                string
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
				if w.revision != "" {
					w.ResponseWriter.Header().Set("X-Aigw-Config-Version", "1")
					w.ResponseWriter.Header().Set("X-Aigw-Channel-Revision", w.revision)
				}
				if w.inputLimit != "" {
					w.ResponseWriter.Header().Set("X-Aigw-Admission-Version", "2")
					w.ResponseWriter.Header().Set("X-Aigw-Input-Limit", w.inputLimit)
				}
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
	events, err := journal.PendingAfter(c.Request.Context(), c.GetInt("id"), 100, c.Param("cursor"))
	if err != nil {
		c.AbortWithStatus(503)
		return
	}
	c.Header("Cache-Control", "no-store")
	c.Header("X-Aigw-Events-Version", "1")
	c.Header("X-Aigw-Paging-Version", "1")
	c.Header("X-Aigw-Admission-Version", "2")
	c.Header("X-Aigw-Embeddings-Version", "1")
	c.Header("X-Aigw-Config-Version", "1")
	if len(events) == 100 {
		c.Header("X-Aigw-Next-Cursor", events[len(events)-1].AttemptID)
	}
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

// Only client-side functions use the reviewed input/output token tariff.
// Provider-hosted tools can have extra charges and remain outside this contract.
func governanceToolsValid(payload map[string]json.RawMessage) bool {
	var tools []struct {
		Type     string `json:"type"`
		Function struct {
			Name       string                     `json:"name"`
			Parameters map[string]json.RawMessage `json:"parameters"`
		} `json:"function"`
	}
	if raw, ok := payload["tools"]; ok && (common.Unmarshal(raw, &tools) != nil || len(tools) == 0 || len(tools) > 8) {
		return false
	}
	names := map[string]bool{}
	for _, tool := range tools {
		if tool.Type != "function" || len(tool.Function.Name) == 0 || len(tool.Function.Name) > 64 || tool.Function.Parameters == nil || names[tool.Function.Name] {
			return false
		}
		names[tool.Function.Name] = true
	}
	if raw, ok := payload["tool_choice"]; ok {
		var choice string
		if len(tools) == 0 || common.Unmarshal(raw, &choice) != nil || (choice != "auto" && choice != "none" && choice != "required") {
			return false
		}
	}
	return true
}

// Validate before native token estimation or media conversion: governed requests
// never fetch arbitrary image URLs and cannot carry unbounded image dimensions.
func governanceInlineImages(payload map[string]json.RawMessage) (bool, bool) {
	var messages []struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
	}
	if common.Unmarshal(payload["messages"], &messages) != nil {
		return false, false
	}
	count, totalBytes := 0, 0
	var pixels int64
	for _, message := range messages {
		content := bytes.TrimSpace(message.Content)
		if len(content) == 0 {
			continue
		}
		if content[0] != '[' {
			var text string
			if common.Unmarshal(content, &text) != nil {
				return false, false
			}
			continue
		}
		var parts []struct {
			Type     string `json:"type"`
			ImageURL *struct {
				URL    string `json:"url"`
				Detail string `json:"detail"`
			} `json:"image_url"`
		}
		if message.Role != "user" || common.Unmarshal(content, &parts) != nil || len(parts) < 1 || len(parts) > 16 {
			return false, false
		}
		for _, part := range parts {
			if part.Type == "text" {
				continue
			}
			if part.Type != "image_url" || part.ImageURL == nil {
				return false, false
			}
			count++
			if count > 4 {
				return false, false
			}
			source := part.ImageURL
			if source.Detail != "" && source.Detail != "auto" && source.Detail != "low" && source.Detail != "high" {
				return false, false
			}
			header, encoded, ok := strings.Cut(source.URL, ",")
			mime := ""
			switch header {
			case "data:image/png;base64":
				mime = "image/png"
			case "data:image/jpeg;base64":
				mime = "image/jpeg"
			default:
				return false, false
			}
			if !ok || len(encoded) > base64.StdEncoding.EncodedLen(256<<10) || strings.ContainsAny(encoded, "\r\n\t ") {
				return false, false
			}
			raw, err := base64.StdEncoding.Strict().DecodeString(encoded)
			if err != nil || len(raw) == 0 || len(raw) > 256<<10 {
				return false, false
			}
			cfg, format, err := image.DecodeConfig(bytes.NewReader(raw))
			if err != nil || "image/"+format != mime || cfg.Width < 1 || cfg.Height < 1 || cfg.Width > 4096 || cfg.Height > 4096 {
				return false, false
			}
			totalBytes += len(raw)
			pixels += int64(cfg.Width) * int64(cfg.Height)
			if totalBytes > 512<<10 || pixels > 16*1024*1024 {
				return false, false
			}
		}
	}
	return count > 0, true
}

// GovernanceChannelConfiguration exposes only an authenticated configuration
// commitment. Credentials, endpoint URLs and raw channel settings are never returned.
func GovernanceChannelConfiguration(c *gin.Context) {
	id, err := strconv.Atoi(c.Param("id"))
	if err != nil || id < 1 || strconv.Itoa(id) != c.Param("id") {
		c.AbortWithStatus(400)
		return
	}
	channel, err := model.GetChannelById(id, true)
	if err != nil {
		c.AbortWithStatus(404)
		return
	}
	revision, err := model.GovernanceChannelRevision(channel)
	if err != nil {
		c.AbortWithStatus(503)
		return
	}
	c.Header("X-Aigw-Config-Version", "1")
	c.Header("Cache-Control", "no-store")
	c.JSON(200, gin.H{"channel_id": id, "revision": revision, "enabled": channel.Status == common.ChannelStatusEnabled})
}
