package peer

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// transitionStep is one step of the decision-table tests.
type transitionStep struct {
	success    bool
	wantHealth Health
}

func TestTrackerTransitions(t *testing.T) {
	cases := []struct {
		name   string
		thresh Thresholds
		steps  []transitionStep
	}{
		{
			name:   "default thresholds happy path",
			thresh: DefaultThresholds(),
			steps: []transitionStep{
				{success: true, wantHealth: HealthHealthy}, // first probe leaves Unknown
				{success: true, wantHealth: HealthHealthy},
				{success: true, wantHealth: HealthHealthy},
			},
		},
		{
			name:   "default thresholds drop to unhealthy after 3 failures",
			thresh: DefaultThresholds(),
			steps: []transitionStep{
				{success: true, wantHealth: HealthHealthy},
				{success: false, wantHealth: HealthHealthy}, // 1 failure
				{success: false, wantHealth: HealthHealthy}, // 2 failures
				{success: false, wantHealth: HealthUnhealthy},
			},
		},
		{
			name:   "unhealthy recovers only after 2 successes",
			thresh: DefaultThresholds(),
			steps: []transitionStep{
				{success: false, wantHealth: HealthUnhealthy}, // first probe from Unknown
				{success: true, wantHealth: HealthUnhealthy},  // 1 success
				{success: true, wantHealth: HealthHealthy},    // 2 successes -> recover
			},
		},
		{
			name:   "alternating success/failure never flips healthy peer",
			thresh: DefaultThresholds(),
			steps: []transitionStep{
				{success: true, wantHealth: HealthHealthy},
				{success: false, wantHealth: HealthHealthy},
				{success: true, wantHealth: HealthHealthy},
				{success: false, wantHealth: HealthHealthy},
				{success: true, wantHealth: HealthHealthy},
			},
		},
		{
			name:   "single failure threshold flips immediately",
			thresh: Thresholds{HealthyToUnhealthy: 1, UnhealthyToHealthy: 1},
			steps: []transitionStep{
				{success: true, wantHealth: HealthHealthy},
				{success: false, wantHealth: HealthUnhealthy},
				{success: true, wantHealth: HealthHealthy},
			},
		},
		{
			name:   "burst of 5 failures stays unhealthy after first transition",
			thresh: DefaultThresholds(),
			steps: []transitionStep{
				{success: true, wantHealth: HealthHealthy},
				{success: false, wantHealth: HealthHealthy},
				{success: false, wantHealth: HealthHealthy},
				{success: false, wantHealth: HealthUnhealthy},
				{success: false, wantHealth: HealthUnhealthy},
				{success: false, wantHealth: HealthUnhealthy},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := NewTracker(Endpoint{ID: "p", Service: "svc", Address: "addr"}, tc.thresh)
			now := time.Unix(0, 0)
			for i, st := range tc.steps {
				now = now.Add(time.Second)
				var got Health
				if st.success {
					got = tr.RecordSuccess(now)
				} else {
					got = tr.RecordFailure(now)
				}
				if got != st.wantHealth {
					t.Fatalf("step %d (success=%v): got %s, want %s", i, st.success, got, st.wantHealth)
				}
			}
		})
	}
}

func TestTrackerSnapshot(t *testing.T) {
	tr := NewTracker(Endpoint{ID: "a", Service: "s", Address: "addr"}, DefaultThresholds())
	tr.RecordSuccess(time.Unix(1, 0))
	tr.RecordSuccess(time.Unix(2, 0))
	snap := tr.Snapshot()
	if snap.ConsecutiveSuccesses != 2 {
		t.Fatalf("expected 2 successes, got %d", snap.ConsecutiveSuccesses)
	}
	if snap.Health != HealthHealthy {
		t.Fatalf("expected healthy, got %s", snap.Health)
	}
	tr.RecordFailure(time.Unix(3, 0))
	snap = tr.Snapshot()
	if snap.ConsecutiveFailures != 1 || snap.ConsecutiveSuccesses != 0 {
		t.Fatalf("snapshot counters wrong: %+v", snap)
	}
}

func TestTrackerInFlight(t *testing.T) {
	tr := NewTracker(Endpoint{ID: "a"}, DefaultThresholds())
	tr.Acquire()
	tr.Acquire()
	tr.Acquire()
	if got := tr.InFlight(); got != 3 {
		t.Fatalf("expected 3 in flight, got %d", got)
	}
	tr.Release()
	if got := tr.InFlight(); got != 2 {
		t.Fatalf("expected 2 in flight, got %d", got)
	}
}

func TestHealthCheckerTick(t *testing.T) {
	var failID atomic.Value
	failID.Store("")

	probe := ProberFunc(func(_ context.Context, ep Endpoint) error {
		if v := failID.Load().(string); v != "" && v == ep.ID {
			return errors.New("simulated failure")
		}
		return nil
	})

	hc := NewHealthChecker(probe, 10*time.Millisecond, 5*time.Millisecond)
	for i := 0; i < 3; i++ {
		hc.Register(NewTracker(Endpoint{ID: string(rune('a' + i))}, DefaultThresholds()))
	}

	// All healthy after one tick
	healthy := hc.Tick(context.Background(), time.Now())
	if healthy != 3 {
		t.Fatalf("expected 3 healthy, got %d", healthy)
	}

	// Fail 'b' until it transitions
	failID.Store("b")
	for i := 0; i < 5; i++ {
		hc.Tick(context.Background(), time.Now())
	}
	got := 0
	for _, tr := range hc.Trackers() {
		if tr.Health() == HealthHealthy {
			got++
		}
	}
	if got != 2 {
		t.Fatalf("expected 2 healthy after failing b, got %d", got)
	}

	// Recover 'b'
	failID.Store("")
	for i := 0; i < 5; i++ {
		hc.Tick(context.Background(), time.Now())
	}
	got = 0
	for _, tr := range hc.Trackers() {
		if tr.Health() == HealthHealthy {
			got++
		}
	}
	if got != 3 {
		t.Fatalf("expected 3 healthy after recovery, got %d", got)
	}
}

func TestHealthCheckerRunCancels(t *testing.T) {
	hc := NewHealthChecker(ProberFunc(func(context.Context, Endpoint) error { return nil }),
		1*time.Millisecond, 1*time.Millisecond)
	hc.Register(NewTracker(Endpoint{ID: "a"}, DefaultThresholds()))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		hc.Run(ctx)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
