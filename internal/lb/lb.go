// Package lb implements the peer-selection strategies used by the sidecar.
//
// All balancers filter out unhealthy peers before applying their per-strategy
// rule. They are safe for concurrent use.
package lb

import (
	"errors"
	"sync/atomic"

	"github.com/SAY-5/edgemesh/internal/peer"
)

// ErrNoHealthyPeers is returned when every peer in the pool is unhealthy.
var ErrNoHealthyPeers = errors.New("lb: no healthy peers")

// Strategy selects one peer from a pool.
type Strategy interface {
	// Pick chooses the next peer. Implementations skip peers that are not
	// HealthHealthy. The optional exclude set is consulted before scoring;
	// trackers whose endpoint ID is present are skipped (used by retry).
	Pick(pool []*peer.Tracker, exclude map[string]struct{}) (*peer.Tracker, error)

	Name() string
}

// RoundRobin distributes load across healthy peers in registration order.
//
// The cursor is shared across calls, so successive Picks rotate through the
// healthy set. When the healthy set changes between calls, the cursor still
// points to the same logical position; we just skip until we find a healthy,
// non-excluded peer.
type RoundRobin struct {
	cursor uint64
}

func NewRoundRobin() *RoundRobin { return &RoundRobin{} }

func (*RoundRobin) Name() string { return "round-robin" }

func (rr *RoundRobin) Pick(pool []*peer.Tracker, exclude map[string]struct{}) (*peer.Tracker, error) {
	if len(pool) == 0 {
		return nil, ErrNoHealthyPeers
	}
	// Filter to the healthy, non-excluded set so the round-robin distribution
	// is even regardless of how many unhealthy peers sit in the pool.
	eligible := pool[:0:0]
	for _, t := range pool {
		if t.Health() != peer.HealthHealthy {
			continue
		}
		if _, skip := exclude[t.Endpoint().ID]; skip {
			continue
		}
		eligible = append(eligible, t)
	}
	if len(eligible) == 0 {
		return nil, ErrNoHealthyPeers
	}
	idx := int(atomic.AddUint64(&rr.cursor, 1)-1) % len(eligible)
	return eligible[idx], nil
}

// LeastPending picks the healthy peer with the smallest in-flight count.
// Ties are broken by registration order.
type LeastPending struct{}

func NewLeastPending() *LeastPending { return &LeastPending{} }

func (*LeastPending) Name() string { return "least-pending" }

func (*LeastPending) Pick(pool []*peer.Tracker, exclude map[string]struct{}) (*peer.Tracker, error) {
	if len(pool) == 0 {
		return nil, ErrNoHealthyPeers
	}
	var best *peer.Tracker
	bestLoad := int64(-1)
	for _, t := range pool {
		if t.Health() != peer.HealthHealthy {
			continue
		}
		if _, skip := exclude[t.Endpoint().ID]; skip {
			continue
		}
		load := t.InFlight()
		if best == nil || load < bestLoad {
			best = t
			bestLoad = load
		}
	}
	if best == nil {
		return nil, ErrNoHealthyPeers
	}
	return best, nil
}

// FromName resolves a strategy name to a fresh strategy instance.
func FromName(name string) Strategy {
	switch name {
	case "least-pending":
		return NewLeastPending()
	default:
		return NewRoundRobin()
	}
}
