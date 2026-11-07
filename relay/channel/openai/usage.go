package openai

import (
	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	relaycommon "github.com/QuantumNous/new-api/relay/common"
	"github.com/QuantumNous/new-api/relaykit/dto"
)

func applyUsagePostProcessing(info *relaycommon.RelayInfo, usage *dto.Usage, responseBody []byte) {
	if info == nil || usage == nil {
		return
	}

	switch info.ChannelType {
	case constant.ChannelTypeDeepSeek:
		if usage.PromptTokensDetails.CachedTokens == 0 && usage.PromptCacheHitTokens != 0 {
			usage.PromptTokensDetails.CachedTokens = usage.PromptCacheHitTokens
		}
	case constant.ChannelTypeZhipu_v4:
		// 智普的cached_tokens在标准位置: usage.prompt_tokens_details.cached_tokens
		if usage.PromptTokensDetails.CachedTokens == 0 {
			if usage.InputTokensDetails != nil && usage.InputTokensDetails.CachedTokens > 0 {
				usage.PromptTokensDetails.CachedTokens = usage.InputTokensDetails.CachedTokens
			} else if cachedTokens, ok := extractCachedTokensFromBody(responseBody); ok {
				usage.PromptTokensDetails.CachedTokens = cachedTokens
			} else if usage.PromptCacheHitTokens > 0 {
				usage.PromptTokensDetails.CachedTokens = usage.PromptCacheHitTokens
			}
		}
	case constant.ChannelTypeMoonshot:
		// Moonshot的cached_tokens在非标准位置: choices[].usage.cached_tokens
		if usage.PromptTokensDetails.CachedTokens == 0 {
			if usage.InputTokensDetails != nil && usage.InputTokensDetails.CachedTokens > 0 {
				usage.PromptTokensDetails.CachedTokens = usage.InputTokensDetails.CachedTokens
			} else if cachedTokens, ok := extractMoonshotCachedTokensFromBody(responseBody); ok {
				usage.PromptTokensDetails.CachedTokens = cachedTokens
			} else if cachedTokens, ok := extractCachedTokensFromBody(responseBody); ok {
				usage.PromptTokensDetails.CachedTokens = cachedTokens
			} else if usage.PromptCacheHitTokens > 0 {
				usage.PromptTokensDetails.CachedTokens = usage.PromptCacheHitTokens
			}
		}
	case constant.ChannelTypeOpenAI:
		if usage.PromptTokensDetails.CachedTokens == 0 {
			if cachedTokens, ok := extractLlamaCachedTokensFromBody(responseBody); ok {
				usage.PromptTokensDetails.CachedTokens = cachedTokens
			}
		}
	}
}

func extractCachedTokensFromBody(body []byte) (int, bool) {
	if len(body) == 0 {
		return 0, false
	}

	var payload struct {
		Usage struct {
			PromptTokensDetails struct {
				CachedTokens *int `json:"cached_tokens"`
			} `json:"prompt_tokens_details"`
			CachedTokens         *int `json:"cached_tokens"`
			PromptCacheHitTokens *int `json:"prompt_cache_hit_tokens"`
		} `json:"usage"`
	}

	if err := common.Unmarshal(body, &payload); err != nil {
		return 0, false
	}

	if payload.Usage.PromptTokensDetails.CachedTokens != nil {
		return *payload.Usage.PromptTokensDetails.CachedTokens, true
	}
	if payload.Usage.CachedTokens != nil {
		return *payload.Usage.CachedTokens, true
	}
	if payload.Usage.PromptCacheHitTokens != nil {
		return *payload.Usage.PromptCacheHitTokens, true
	}
	return 0, false
}

// extractMoonshotCachedTokensFromBody 从Moonshot的非标准位置提取cached_tokens
// Moonshot的流式响应格式: {"choices":[{"usage":{"cached_tokens":111}}]}
func extractMoonshotCachedTokensFromBody(body []byte) (int, bool) {
	if len(body) == 0 {
		return 0, false
	}

	var payload struct {
		Choices []struct {
			Usage struct {
				CachedTokens *int `json:"cached_tokens"`
			} `json:"usage"`
		} `json:"choices"`
	}

	if err := common.Unmarshal(body, &payload); err != nil {
		return 0, false
	}

	// 遍历choices查找cached_tokens
	for _, choice := range payload.Choices {
		if choice.Usage.CachedTokens != nil && *choice.Usage.CachedTokens > 0 {
			return *choice.Usage.CachedTokens, true
		}
	}

	return 0, false
}

// extractLlamaCachedTokensFromBody 从llama.cpp的非标准位置提取cache_n
func extractLlamaCachedTokensFromBody(body []byte) (int, bool) {
	if len(body) == 0 {
		return 0, false
	}

	var payload struct {
		Timings struct {
			CachedTokens *int `json:"cache_n"`
		} `json:"timings"`
	}

	if err := common.Unmarshal(body, &payload); err != nil {
		return 0, false
	}

	if payload.Timings.CachedTokens == nil {
		return 0, false
	}
	return *payload.Timings.CachedTokens, true
}

// captureUpstreamUsage reads facts before any estimation or vendor normalization.
// Missing, fractional, negative, oversized or inconsistent counters are not
// evidence. Explicit zero is valid and differs from an omitted counter.
func captureUpstreamUsage(body []byte, complete bool) *relaycommon.UpstreamUsageEvidence {
	var payload struct {
		Usage *struct {
			Prompt     *int64 `json:"prompt_tokens"`
			Completion *int64 `json:"completion_tokens"`
			Total      *int64 `json:"total_tokens"`
			Hit        *int64 `json:"prompt_cache_hit_tokens"`
			Miss       *int64 `json:"prompt_cache_miss_tokens"`
			Details    *struct {
				Cached *int64 `json:"cached_tokens"`
				Write  *int64 `json:"cached_creation_tokens"`
			} `json:"prompt_tokens_details"`
		} `json:"usage"`
	}
	if err := common.Unmarshal(body, &payload); err != nil || payload.Usage == nil {
		return nil
	}
	u := payload.Usage
	if u.Prompt == nil || u.Completion == nil || u.Total == nil {
		return nil
	}
	const maxTokens = int64(1<<31 - 1)
	if *u.Prompt < 0 || *u.Completion < 0 || *u.Total < 0 ||
		*u.Prompt > maxTokens || *u.Completion > maxTokens || *u.Total > maxTokens ||
		*u.Prompt+*u.Completion != *u.Total {
		return nil
	}
	version := 0
	var read, write int64
	if u.Details != nil && u.Details.Write != nil && u.Details.Cached == nil {
		return nil
	}
	if u.Details != nil && u.Details.Cached != nil {
		version = 1
		read = *u.Details.Cached
		if u.Details.Write != nil {
			write = *u.Details.Write
		}
	}
	if u.Hit != nil || u.Miss != nil {
		if u.Hit == nil || u.Miss == nil || *u.Hit < 0 || *u.Miss < 0 || *u.Hit > maxTokens || *u.Miss > maxTokens || *u.Hit+*u.Miss != *u.Prompt || (version == 1 && read != *u.Hit) {
			return nil
		}
		version = 1
		read = *u.Hit
	}
	if read < 0 || write < 0 || read > maxTokens || write > maxTokens || read+write > *u.Prompt {
		return nil
	}
	return &relaycommon.UpstreamUsageEvidence{
		CacheDetailsVersion: version, CacheReadTokens: read, CacheWriteTokens: write,
		PromptTokens: int(*u.Prompt), CompletionTokens: int(*u.Completion),
		TotalTokens: int(*u.Total), Complete: complete,
	}
}

// Embeddings define no generated output; absence of completion_tokens is valid
// only for this protocol, never a substitute for missing Chat usage.
func captureEmbeddingUsage(body []byte, complete bool) *relaycommon.UpstreamUsageEvidence {
	var payload struct {
		Usage map[string]*int64 `json:"usage"`
	}
	if common.Unmarshal(body, &payload) != nil {
		return nil
	}
	for key, value := range payload.Usage {
		if value == nil {
			return nil
		}
		if key != "prompt_tokens" && key != "total_tokens" && key != "completion_tokens" {
			return nil
		}
	}
	prompt, p := payload.Usage["prompt_tokens"]
	total, t := payload.Usage["total_tokens"]
	if !p || !t || prompt == nil || total == nil || *prompt < 1 || *prompt > 1<<31-1 || *total != *prompt || (payload.Usage["completion_tokens"] != nil && *payload.Usage["completion_tokens"] != 0) {
		return nil
	}
	return &relaycommon.UpstreamUsageEvidence{PromptTokens: int(*prompt), TotalTokens: int(*total), Complete: complete}
}
