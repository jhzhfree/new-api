package common

import (
	"errors"
	"github.com/QuantumNous/new-api/common"
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
