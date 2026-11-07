package common

import (
	"bytes"
	"encoding/base64"
	"errors"
	"image"
	_ "image/jpeg"
	_ "image/png"
	"strings"

	"github.com/QuantumNous/new-api/common"
	"github.com/QuantumNous/new-api/constant"
	"github.com/QuantumNous/new-api/relaykit/dto"
	"github.com/QuantumNous/new-api/relaykit/types"
	"github.com/QuantumNous/new-api/tokenkit"
)

// ValidateGovernedOutputBound checks the actual wire request after converters,
// model modifiers and overrides. No default may increase the approved ceiling.
func ValidateGovernedOutputBound(body []byte, ceiling uint) error {
	var request struct {
		MaxTokens           *uint `json:"max_tokens"`
		MaxCompletionTokens *uint `json:"max_completion_tokens"`
	}
	if ceiling == 0 || common.Unmarshal(body, &request) != nil {
		return errors.New("invalid governed request")
	}
	bound := request.MaxTokens
	if request.MaxCompletionTokens != nil {
		if bound != nil {
			return errors.New("ambiguous output bound")
		}
		bound = request.MaxCompletionTokens
	}
	if bound == nil || *bound == 0 || *bound > ceiling {
		return errors.New("output bound exceeds approval")
	}
	return nil
}

// GovernedInputEstimate measures the converted wire request, never cached billing
// estimates. It is a local admission estimate, not supplier billing evidence.
func GovernedInputEstimate(body []byte, channelType int) (int, error) {
	invalid := errors.New("unsupported governed input estimate")
	if len(body) > 1<<20 {
		return 0, invalid
	}
	var meta *types.TokenCountMeta
	var model string
	switch channelType {
	case constant.ChannelTypeAnthropic:
		var request dto.ClaudeRequest
		if common.Unmarshal(body, &request) != nil {
			return 0, invalid
		}
		model = request.Model
		meta = request.GetTokenCountMeta()
	case constant.ChannelTypeOpenAI, constant.ChannelTypeDeepSeek:
		var request dto.GeneralOpenAIRequest
		if common.Unmarshal(body, &request) != nil {
			return 0, invalid
		}
		model = request.Model
		meta = request.GetTokenCountMeta()
	default:
		return 0, invalid
	}
	if model == "" || meta == nil || meta.MessagesCount < 1 {
		return 0, invalid
	}
	total := tokenkit.Count(model, meta.CombineText) + meta.ToolsCount*8 + meta.MessagesCount*3 + meta.NameCount*3 + 3
	if total < 0 || len(meta.Files) > 4 {
		return 0, invalid
	}
	for _, file := range meta.Files {
		if file == nil || file.FileType != types.FileTypeImage {
			return 0, invalid
		}
		source, ok := file.Source.(*types.Base64Source)
		if !ok {
			return 0, invalid
		}
		encoded, mime := source.Base64Data, source.MimeType
		if strings.HasPrefix(encoded, "data:") {
			header, payload, found := strings.Cut(encoded, ",")
			if !found || !strings.HasSuffix(header, ";base64") {
				return 0, invalid
			}
			declared := strings.TrimSuffix(strings.TrimPrefix(header, "data:"), ";base64")
			if mime != "" && mime != declared {
				return 0, invalid
			}
			mime, encoded = declared, payload
		}
		if (mime != "image/png" && mime != "image/jpeg") || len(encoded) > base64.StdEncoding.EncodedLen(256<<10) {
			return 0, invalid
		}
		raw, err := base64.StdEncoding.Strict().DecodeString(encoded)
		if err != nil {
			return 0, invalid
		}
		cfg, format, err := image.DecodeConfig(bytes.NewReader(raw))
		if err != nil || "image/"+format != mime || cfg.Width < 1 || cfg.Height < 1 || cfg.Width > 4096 || cfg.Height > 4096 {
			return 0, invalid
		}
		total += tokenkit.ImageTokens(model, tokenkit.Image{Width: cfg.Width, Height: cfg.Height, Detail: file.Detail})
	}
	return total, nil
}
