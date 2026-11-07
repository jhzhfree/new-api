package common

import (
	"errors"
	"sync/atomic"
)

// SingleAttempt is an in-memory dispatch guard for a trusted execution context.
// It is never populated from client JSON or headers. It bounds HTTP dispatches
// within one process; durable cross-process deduplication is a separate concern.
// A guard must not be copied or reused for another business attempt.
type SingleAttempt struct {
	started atomic.Bool
	before  func() error
}

// NewDurableSingleAttempt requires a durable dispatch fence to commit before
// transport I/O. A failed or ambiguous commit consumes this in-memory guard too.
func NewDurableSingleAttempt(before func() error) (*SingleAttempt, error) {
	if before == nil {
		return nil, errors.New("durable dispatch fence required")
	}
	return &SingleAttempt{before: before}, nil
}

func (s *SingleAttempt) Begin() error {
	if s == nil || !s.started.CompareAndSwap(false, true) {
		return errors.New("physical dispatch already attempted")
	}
	if s.before != nil && s.before() != nil {
		return errors.New("durable dispatch fence unavailable")
	}
	return nil
}
