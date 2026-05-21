// Package retry implements the per-RPC retry classifier and backoff policy.
//
// Classification follows the standard gRPC guidance for safe-to-retry codes
// (UNAVAILABLE, DEADLINE_EXCEEDED, ABORTED) and never retries codes that
// indicate a bug or a permission problem. A per-method idempotency flag from
// the YAML config gates whether non-idempotent methods retry at all.
package retry

import (
	"context"
	"errors"
	"math/rand"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Policy controls retry behaviour.
type Policy struct {
	MaxAttempts uint32        // total attempts including the first
	Base        time.Duration // first backoff
	Multiplier  float64       // applied between attempts
	Max         time.Duration // cap on a single sleep
	JitterFrac  float64       // +/- fraction of the computed sleep, e.g. 0.2 for +/-20%
}

// DefaultPolicy returns the documented defaults: 3 attempts, 50ms initial
// backoff, 4x multiplier (50/200/800), 1s cap, 20% jitter.
func DefaultPolicy() Policy {
	return Policy{
		MaxAttempts: 3,
		Base:        50 * time.Millisecond,
		Multiplier:  4.0,
		Max:         time.Second,
		JitterFrac:  0.2,
	}
}

// Classify returns whether the given error is retriable.
// A nil error is not retriable (the call succeeded).
func Classify(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	if !ok {
		// non-gRPC error (e.g. transport-level): treat as retriable
		return true
	}
	switch st.Code() {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted:
		return true
	default:
		return false
	}
}

// IsTerminal returns true when the error indicates a definitive failure that
// should not be retried.
func IsTerminal(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	if !ok {
		return false
	}
	switch st.Code() {
	case codes.InvalidArgument, codes.NotFound, codes.PermissionDenied,
		codes.Unauthenticated, codes.AlreadyExists, codes.FailedPrecondition,
		codes.OutOfRange, codes.Unimplemented:
		return true
	default:
		return false
	}
}

// Backoff returns the sleep before attempt index (0-based: attempt 1 is the
// first retry). Pure function so tests can pin the random source.
func Backoff(p Policy, attempt int, rng *rand.Rand) time.Duration {
	if attempt < 0 {
		attempt = 0
	}
	d := float64(p.Base)
	for i := 0; i < attempt; i++ {
		d *= p.Multiplier
	}
	if d > float64(p.Max) {
		d = float64(p.Max)
	}
	if p.JitterFrac > 0 && rng != nil {
		// uniform jitter in [-JitterFrac, +JitterFrac]
		j := (rng.Float64()*2 - 1) * p.JitterFrac
		d *= (1 + j)
	}
	if d < 0 {
		d = 0
	}
	return time.Duration(d)
}

// Do executes fn up to p.MaxAttempts times, applying the classifier between
// attempts. If idempotent is false, retries are disabled regardless of the
// classifier (matches the YAML method.idempotent flag).
//
// Returns the final result of fn together with the number of attempts used
// (1 means "no retries"). Honours ctx cancellation between attempts.
func Do(ctx context.Context, p Policy, idempotent bool, rng *rand.Rand, fn func(ctx context.Context, attempt int) error) (attempts int, err error) {
	if p.MaxAttempts == 0 {
		p.MaxAttempts = 1
	}
	for attempt := 0; attempt < int(p.MaxAttempts); attempt++ {
		err = fn(ctx, attempt)
		attempts = attempt + 1
		if err == nil {
			return attempts, nil
		}
		if !idempotent {
			return attempts, err
		}
		if !Classify(err) {
			return attempts, err
		}
		if attempt == int(p.MaxAttempts)-1 {
			return attempts, err
		}
		sleep := Backoff(p, attempt, rng)
		t := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			t.Stop()
			return attempts, errors.Join(err, ctx.Err())
		case <-t.C:
		}
	}
	return attempts, err
}
