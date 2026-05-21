// Package peer maintains the state machine for a single mesh peer.
//
// Each peer is described by an Endpoint (immutable) and a State (mutable).
// The Tracker wraps the state with the success/failure counters consumed by
// the health checker; it is safe for concurrent use.
package peer

import (
	"sync"
	"sync/atomic"
	"time"
)

// Health captures the externally visible health of a peer.
type Health int32

const (
	HealthUnknown Health = iota
	HealthHealthy
	HealthUnhealthy
)

func (h Health) String() string {
	switch h {
	case HealthHealthy:
		return "healthy"
	case HealthUnhealthy:
		return "unhealthy"
	default:
		return "unknown"
	}
}

// Endpoint identifies a peer at the wire level.
type Endpoint struct {
	ID      string
	Service string
	Address string
}

// Thresholds controls the state machine transitions.
//
// A peer becomes Unhealthy after HealthyToUnhealthy consecutive failures,
// and returns to Healthy after UnhealthyToHealthy consecutive successes.
// A peer in HealthUnknown moves to Healthy on the first success and to
// Unhealthy on the first failure (this matches gRPC's outlier detection
// initialisation: assume nothing until we have evidence).
type Thresholds struct {
	HealthyToUnhealthy uint32
	UnhealthyToHealthy uint32
}

// DefaultThresholds returns the documented defaults: 3 consecutive failures
// to become unhealthy, 2 consecutive successes to recover.
func DefaultThresholds() Thresholds {
	return Thresholds{HealthyToUnhealthy: 3, UnhealthyToHealthy: 2}
}

// Tracker is the per-peer state held by the sidecar.
type Tracker struct {
	endpoint  Endpoint
	thresh    Thresholds
	mu        sync.Mutex
	health    Health
	successes uint32
	failures  uint32
	lastProbe time.Time
	inFlight  int64
}

// NewTracker constructs a Tracker with the given endpoint and thresholds.
func NewTracker(ep Endpoint, t Thresholds) *Tracker {
	if t.HealthyToUnhealthy == 0 {
		t.HealthyToUnhealthy = 3
	}
	if t.UnhealthyToHealthy == 0 {
		t.UnhealthyToHealthy = 2
	}
	return &Tracker{endpoint: ep, thresh: t, health: HealthUnknown}
}

// Endpoint returns the immutable endpoint.
func (t *Tracker) Endpoint() Endpoint { return t.endpoint }

// Health returns the current externally visible health.
func (t *Tracker) Health() Health {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.health
}

// Snapshot returns a copy of the current state.
type Snapshot struct {
	Endpoint             Endpoint
	Health               Health
	ConsecutiveSuccesses uint32
	ConsecutiveFailures  uint32
	LastProbe            time.Time
	InFlight             int64
}

// Snapshot returns a consistent view of the tracker's counters.
func (t *Tracker) Snapshot() Snapshot {
	t.mu.Lock()
	defer t.mu.Unlock()
	return Snapshot{
		Endpoint:             t.endpoint,
		Health:               t.health,
		ConsecutiveSuccesses: t.successes,
		ConsecutiveFailures:  t.failures,
		LastProbe:            t.lastProbe,
		InFlight:             atomic.LoadInt64(&t.inFlight),
	}
}

// RecordSuccess advances the state machine on a successful probe.
// Returns the resulting Health for caller-side logging.
func (t *Tracker) RecordSuccess(now time.Time) Health {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastProbe = now
	t.failures = 0
	t.successes++
	switch t.health {
	case HealthUnknown:
		t.health = HealthHealthy
	case HealthUnhealthy:
		if t.successes >= t.thresh.UnhealthyToHealthy {
			t.health = HealthHealthy
		}
	case HealthHealthy:
		// stay
	}
	return t.health
}

// RecordFailure advances the state machine on a failed probe.
func (t *Tracker) RecordFailure(now time.Time) Health {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.lastProbe = now
	t.successes = 0
	t.failures++
	switch t.health {
	case HealthUnknown:
		t.health = HealthUnhealthy
	case HealthHealthy:
		if t.failures >= t.thresh.HealthyToUnhealthy {
			t.health = HealthUnhealthy
		}
	case HealthUnhealthy:
		// stay
	}
	return t.health
}

// InFlight returns the live in-flight RPC count for the peer.
func (t *Tracker) InFlight() int64 { return atomic.LoadInt64(&t.inFlight) }

// Acquire marks the start of an in-flight RPC.
func (t *Tracker) Acquire() { atomic.AddInt64(&t.inFlight, 1) }

// Release marks the end of an in-flight RPC.
func (t *Tracker) Release() { atomic.AddInt64(&t.inFlight, -1) }
