package retry

import (
	"context"
	"errors"
	"math/rand"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestClassify(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil is not retriable", nil, false},
		{"UNAVAILABLE retriable", status.Error(codes.Unavailable, "x"), true},
		{"DEADLINE_EXCEEDED retriable", status.Error(codes.DeadlineExceeded, "x"), true},
		{"ABORTED retriable", status.Error(codes.Aborted, "x"), true},
		{"INVALID_ARGUMENT not retriable", status.Error(codes.InvalidArgument, "x"), false},
		{"NOT_FOUND not retriable", status.Error(codes.NotFound, "x"), false},
		{"PERMISSION_DENIED not retriable", status.Error(codes.PermissionDenied, "x"), false},
		{"plain non-grpc error retriable", errors.New("transport blew up"), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := Classify(c.err); got != c.want {
				t.Fatalf("Classify(%v) = %v, want %v", c.err, got, c.want)
			}
		})
	}
}

func TestIsTerminal(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{status.Error(codes.InvalidArgument, "x"), true},
		{status.Error(codes.NotFound, "x"), true},
		{status.Error(codes.PermissionDenied, "x"), true},
		{status.Error(codes.Unimplemented, "x"), true},
		{status.Error(codes.Unavailable, "x"), false},
		{nil, false},
		{errors.New("plain"), false},
	}
	for _, c := range cases {
		if got := IsTerminal(c.err); got != c.want {
			t.Fatalf("IsTerminal(%v) = %v, want %v", c.err, got, c.want)
		}
	}
}

func TestBackoffMonotonic(t *testing.T) {
	p := Policy{MaxAttempts: 4, Base: 10 * time.Millisecond, Multiplier: 4, Max: time.Second}
	prev := time.Duration(-1)
	for i := 0; i < 3; i++ {
		d := Backoff(p, i, nil)
		if d <= prev {
			t.Fatalf("backoff not monotonic at attempt %d: prev=%v d=%v", i, prev, d)
		}
		prev = d
	}
}

func TestBackoffCap(t *testing.T) {
	p := Policy{Base: 100 * time.Millisecond, Multiplier: 10, Max: 200 * time.Millisecond}
	d := Backoff(p, 5, nil)
	if d > p.Max {
		t.Fatalf("expected <= %v, got %v", p.Max, d)
	}
}

func TestBackoffJitterBounded(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	p := Policy{Base: 100 * time.Millisecond, Multiplier: 2, Max: time.Second, JitterFrac: 0.5}
	for i := 0; i < 100; i++ {
		d := Backoff(p, 1, rng)
		// expected ~200ms +/- 50%
		if d < 100*time.Millisecond || d > 300*time.Millisecond {
			t.Fatalf("jittered backoff out of bounds: %v", d)
		}
	}
}

func TestDoSucceedsOnFirstAttempt(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	attempts, err := Do(context.Background(), DefaultPolicy(), true, rng,
		func(context.Context, int) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", attempts)
	}
}

func TestDoRetriesUntilSuccess(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	p := Policy{MaxAttempts: 5, Base: time.Microsecond, Multiplier: 2, Max: time.Millisecond}
	attempts, err := Do(context.Background(), p, true, rng,
		func(_ context.Context, a int) error {
			if a < 2 {
				return status.Error(codes.Unavailable, "transient")
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if attempts != 3 {
		t.Fatalf("expected 3 attempts, got %d", attempts)
	}
}

func TestDoStopsOnTerminal(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	p := Policy{MaxAttempts: 5, Base: time.Microsecond, Max: time.Millisecond, Multiplier: 2}
	calls := 0
	attempts, err := Do(context.Background(), p, true, rng,
		func(context.Context, int) error {
			calls++
			return status.Error(codes.NotFound, "gone")
		})
	if err == nil {
		t.Fatal("expected error")
	}
	if attempts != 1 || calls != 1 {
		t.Fatalf("expected 1 attempt on terminal error, got attempts=%d calls=%d", attempts, calls)
	}
}

func TestDoNonIdempotentDoesNotRetry(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	calls := 0
	_, err := Do(context.Background(), DefaultPolicy(), false, rng,
		func(context.Context, int) error {
			calls++
			return status.Error(codes.Unavailable, "x")
		})
	if err == nil {
		t.Fatal("expected error")
	}
	if calls != 1 {
		t.Fatalf("non-idempotent should not retry: got %d calls", calls)
	}
}

// TestDoTerminatesUnderRandomFailures is the property-style test: for any
// random sequence of transient failures, Do terminates within MaxAttempts.
func TestDoTerminatesUnderRandomFailures(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	p := Policy{MaxAttempts: 6, Base: time.Microsecond, Multiplier: 2, Max: time.Millisecond, JitterFrac: 0.2}
	for trial := 0; trial < 500; trial++ {
		failCount := rng.Intn(int(p.MaxAttempts) + 2) // 0..MaxAttempts+1
		seen := 0
		attempts, err := Do(context.Background(), p, true, rng,
			func(context.Context, int) error {
				if seen < failCount {
					seen++
					return status.Error(codes.Unavailable, "x")
				}
				return nil
			})
		if attempts > int(p.MaxAttempts) {
			t.Fatalf("trial %d: attempts=%d exceeded MaxAttempts=%d", trial, attempts, p.MaxAttempts)
		}
		if failCount < int(p.MaxAttempts) && err != nil {
			t.Fatalf("trial %d: should have succeeded after %d failures, got %v", trial, failCount, err)
		}
	}
}

func TestDoHonoursContext(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	p := Policy{MaxAttempts: 10, Base: 50 * time.Millisecond, Multiplier: 2, Max: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before the first call
	_, err := Do(ctx, p, true, rng, func(context.Context, int) error {
		return status.Error(codes.Unavailable, "x")
	})
	if err == nil {
		t.Fatal("expected context error")
	}
}
