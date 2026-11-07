package claude

import (
	"github.com/QuantumNous/new-api/common"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
)

// Evidence is collected from the provider wire data before converter estimates
// or billing normalization. Anthropic input excludes cache reads and writes.
type rawClaudeUsage struct {
	Input       *int64           `json:"input_tokens"`
	Output      *int64           `json:"output_tokens"`
	Read        *int64           `json:"cache_read_input_tokens"`
	Write       *int64           `json:"cache_creation_input_tokens"`
	ServerTools map[string]int64 `json:"server_tool_use"`
}
type claudeUsageEvidence struct {
	input, output, read, write   int64
	started, delta, stopped, bad bool
}

func (e *claudeUsageEvidence) update(u *rawClaudeUsage, initial bool) {
	if u == nil || u.Output == nil || initial && u.Input == nil {
		e.bad = true
		return
	}
	for _, count := range []*int64{u.Input, u.Output, u.Read, u.Write} {
		if count != nil && (*count < 0 || *count > 1000000000) {
			e.bad = true
			return
		}
	}
	for _, count := range u.ServerTools {
		if count != 0 {
			e.bad = true
			return
		}
	}
	if !initial && *u.Output < e.output {
		e.bad = true
		return
	}
	if u.Input != nil {
		e.input = *u.Input
	}
	if u.Read != nil {
		e.read = *u.Read
	}
	if u.Write != nil {
		e.write = *u.Write
	}
	e.output = *u.Output
	if e.input+e.read+e.write+e.output > 1000000000 {
		e.bad = true
	}
}
func (e *claudeUsageEvidence) observe(data []byte) {
	var frame struct {
		Type         string `json:"type"`
		ContentBlock *struct {
			Type string `json:"type"`
		} `json:"content_block"`
		Delta *struct {
			Type string `json:"type"`
		} `json:"delta"`
		Message *struct {
			Usage *rawClaudeUsage `json:"usage"`
		} `json:"message"`
		Usage *rawClaudeUsage `json:"usage"`
	}
	if common.Unmarshal(data, &frame) != nil || e.stopped {
		e.bad = true
		return
	}
	switch frame.Type {
	case "message_start":
		if e.started || frame.Message == nil {
			e.bad = true
			return
		}
		e.started = true
		e.update(frame.Message.Usage, true)
	case "message_delta":
		if !e.started {
			e.bad = true
			return
		}
		e.delta = true
		e.update(frame.Usage, false)
	case "message_stop":
		if !e.started || !e.delta {
			e.bad = true
		}
		e.stopped = true
	case "error":
		e.bad = true
	case "content_block_start", "content_block_delta", "content_block_stop", "ping":
		if frame.Type == "content_block_start" && (frame.ContentBlock == nil || (frame.ContentBlock.Type != "text" && frame.ContentBlock.Type != "tool_use")) {
			e.bad = true
		}
		if frame.Type == "content_block_delta" && (frame.Delta == nil || (frame.Delta.Type != "text_delta" && frame.Delta.Type != "input_json_delta")) {
			e.bad = true
		}
		if !e.started && frame.Type != "ping" {
			e.bad = true
		}
	default:
		e.bad = true
	}
}
func (e *claudeUsageEvidence) result(complete bool) *relaycommon.UpstreamUsageEvidence {
	if e.bad || !e.started || !e.delta || !e.stopped || !complete {
		return nil
	}
	input := e.input + e.read + e.write
	return &relaycommon.UpstreamUsageEvidence{PromptTokens: int(input), CompletionTokens: int(e.output), TotalTokens: int(input + e.output), Complete: true}
}
func claudeJSONEvidence(data []byte) *relaycommon.UpstreamUsageEvidence {
	var response struct {
		Content []struct {
			Type string `json:"type"`
		} `json:"content"`
		Type       string          `json:"type"`
		StopReason string          `json:"stop_reason"`
		Usage      *rawClaudeUsage `json:"usage"`
	}
	if common.Unmarshal(data, &response) != nil || response.Type != "message" || response.StopReason == "" {
		return nil
	}
	for _, block := range response.Content {
		if block.Type != "text" && block.Type != "tool_use" {
			return nil
		}
	}
	evidence := &claudeUsageEvidence{started: true, delta: true, stopped: true}
	evidence.update(response.Usage, true)
	return evidence.result(true)
}
