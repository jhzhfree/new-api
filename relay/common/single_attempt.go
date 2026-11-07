package common

import "sync/atomic"

// SingleAttempt is an in-memory dispatch guard for a trusted execution context.
// It is never populated from client JSON or headers. It bounds HTTP dispatches
// within one process; durable cross-process deduplication is a separate concern.
// A guard must not be copied or reused for another business attempt.
type SingleAttempt struct {
	started atomic.Bool
}

func (s *SingleAttempt) Begin() bool {
	return s != nil && s.started.CompareAndSwap(false, true)
}
