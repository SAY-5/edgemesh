// Package chaos models the network conditions used by the topology
// simulator. The fault injector exposes a single concurrency-safe API: given
// a source / destination pair, return either "deliver" or "drop". Latency is
// optional and bounded by Settings.MaxDelay.
//
// The chaos package is entirely synchronous and in-memory; nothing in this
// file talks to the network. It is consumed by the topology simulator to
// decide what happens when a sidecar tries to reach a peer.
package chaos

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// Settings parametrise the chaos engine.
type Settings struct {
	DropProb      float64       // 0.0 .. 1.0 baseline packet drop probability
	MaxDelay      time.Duration // upper bound on per-call latency injection
	PartitionProb float64       // probability that a link is partitioned at any time
	NodeDownProb  float64       // probability a node is fully unreachable
	Seed          int64
}

// DefaultSettings returns the chaos profile used by the in-CI smoke test.
// Locally we drive it harder via CHAOS_SCENARIOS.
func DefaultSettings() Settings {
	return Settings{
		DropProb:      0.05,
		MaxDelay:      10 * time.Millisecond,
		PartitionProb: 0.0, // partitions are toggled explicitly by the harness
		NodeDownProb:  0.0,
		Seed:          1,
	}
}

// linkKey identifies a directed source->dest edge.
type linkKey struct{ src, dst string }

// FaultInjector decides whether a packet from src to dst gets delivered.
//
// It also models global node-down state (a downed node drops all traffic in
// either direction) and per-link partitions (asymmetric: partition(a, b)
// affects only a->b).
type FaultInjector struct {
	mu         sync.RWMutex
	settings   Settings
	rng        *rand.Rand
	down       map[string]bool
	partitions map[linkKey]bool
}

// New constructs a FaultInjector with the given settings.
func New(s Settings) *FaultInjector {
	return &FaultInjector{
		settings:   s,
		rng:        rand.New(rand.NewSource(s.Seed)),
		down:       make(map[string]bool),
		partitions: make(map[linkKey]bool),
	}
}

// SetNodeDown toggles the global reachability of a node.
func (f *FaultInjector) SetNodeDown(node string, down bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if down {
		f.down[node] = true
		return
	}
	delete(f.down, node)
}

// Partition severs the directed link src -> dst.
func (f *FaultInjector) Partition(src, dst string, partitioned bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	k := linkKey{src, dst}
	if partitioned {
		f.partitions[k] = true
		return
	}
	delete(f.partitions, k)
}

// ClearAll removes every partition and brings every node up. Used between
// scenarios.
func (f *FaultInjector) ClearAll() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.down = make(map[string]bool)
	f.partitions = make(map[linkKey]bool)
}

// Deliver decides whether a single message from src to dst should be
// delivered. Returns nil to deliver, or an error to drop. The optional sleep
// is applied by the caller (we keep this synchronous so tests stay fast).
func (f *FaultInjector) Deliver(ctx context.Context, src, dst string) error {
	f.mu.RLock()
	if f.down[src] || f.down[dst] {
		f.mu.RUnlock()
		return fmt.Errorf("chaos: node %s or %s is down", src, dst)
	}
	if f.partitions[linkKey{src, dst}] {
		f.mu.RUnlock()
		return fmt.Errorf("chaos: link %s->%s partitioned", src, dst)
	}
	s := f.settings
	f.mu.RUnlock()

	// Random drop and latency, gated by RNG. The lock is held only while
	// reading settings so concurrent callers do not contend.
	f.mu.Lock()
	drop := f.rng.Float64() < s.DropProb
	var sleep time.Duration
	if s.MaxDelay > 0 {
		sleep = time.Duration(f.rng.Int63n(int64(s.MaxDelay)))
	}
	f.mu.Unlock()

	if sleep > 0 {
		t := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case <-t.C:
		}
	}
	if drop {
		return fmt.Errorf("chaos: random drop %s->%s", src, dst)
	}
	return nil
}

// Snapshot returns a copy of the current fault state.
type Snapshot struct {
	Down       []string
	Partitions []string // formatted "src->dst"
}

func (f *FaultInjector) Snapshot() Snapshot {
	f.mu.RLock()
	defer f.mu.RUnlock()
	s := Snapshot{}
	for n := range f.down {
		s.Down = append(s.Down, n)
	}
	for k := range f.partitions {
		s.Partitions = append(s.Partitions, fmt.Sprintf("%s->%s", k.src, k.dst))
	}
	return s
}
