package gbus

import (
	"errors"
	"fmt"
)

// The stable errors. Callers match on these instead of parsing driver strings,
// because driver messages change between versions.
var (
	// ErrClosed means the producer or consumer was closed.
	ErrClosed = errors.New("gbus: closed")
	// ErrTimeout means the broker did not answer in time. The outcome is unknown:
	// the message may or may not be persisted, so a retry needs a business
	// idempotency key.
	ErrTimeout = errors.New("gbus: timeout")
	// ErrQueueFull means the driver's bounded buffer is full; caller should back
	// off rather than spin.
	ErrQueueFull = errors.New("gbus: queue full")
	// ErrMessageTooLarge means the value exceeds the configured ceiling.
	ErrMessageTooLarge = errors.New("gbus: message too large")
	// ErrUnsupportedSemantics means the driver cannot honour the requested
	// configuration (for example asking Pub/Sub for consumer groups).
	ErrUnsupportedSemantics = errors.New("gbus: unsupported semantics")
	// ErrPermanent means retrying the same call cannot succeed.
	ErrPermanent = errors.New("gbus: permanent error")
)

// Temporary wraps a driver failure that may succeed on retry.
func Temporary(err error) error {
	if err == nil {
		return nil
	}
	return &classified{kind: ErrTimeout, err: err}
}

// Permanent wraps a failure that will not succeed on retry.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &classified{kind: ErrPermanent, err: err}
}

type classified struct {
	kind error
	err  error
}

func (c *classified) Error() string { return fmt.Sprintf("%v: %v", c.kind, c.err) }

// Unwrap exposes both the classification and the driver error.
func (c *classified) Unwrap() []error { return []error{c.kind, c.err} }

// IsTemporary reports whether retrying may help. A closed bus or a permanent
// failure never becomes temporary.
func IsTemporary(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrPermanent) || errors.Is(err, ErrClosed) ||
		errors.Is(err, ErrMessageTooLarge) || errors.Is(err, ErrUnsupportedSemantics) {
		return false
	}
	return errors.Is(err, ErrTimeout) || errors.Is(err, ErrQueueFull)
}

// IsPermanent reports the opposite of IsTemporary for a non-nil error.
func IsPermanent(err error) bool {
	return err != nil && !IsTemporary(err)
}
