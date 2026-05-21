// Package topology is the in-process simulator used by the chaos suite.
//
// It models N sidecars + N application services in goroutines on a shared
// process, with all traffic mediated by chaos.FaultInjector. The simulator
// does not use real sockets: every "send" is a function call that consults
// the fault injector and either returns an error (drop) or invokes the
// destination's handler.
//
// This is the load-bearing test, intentionally written without gRPC so that
// the chaos behaviour, the LB, the retry classifier, and the health checker
// can be tested independently of any networking concern.
package topology

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SAY-5/edgemesh/internal/chaos"
	"github.com/SAY-5/edgemesh/internal/lb"
	"github.com/SAY-5/edgemesh/internal/peer"
	"github.com/SAY-5/edgemesh/internal/retry"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Handler is the per-node application handler. The simulator invokes the
// destination's handler on a successful delivery.
type Handler func(ctx context.Context, from, to string, payload string) (string, error)

// Node represents a single sidecar + its co-located service.
type Node struct {
	ID       string
	tracker  map[string]*peer.Tracker // peerID -> tracker (this node's view)
	handler  Handler
	strategy lb.Strategy
	policy   retry.Policy
	rngMu    sync.Mutex
	rng      *rand.Rand
}

// Topology owns all nodes and the chaos injector. Safe for concurrent use.
type Topology struct {
	mu     sync.RWMutex
	nodes  map[string]*Node
	chaos  *chaos.FaultInjector
	logger func(string, ...any)
}

// New builds a Topology with the given chaos injector.
func New(c *chaos.FaultInjector, logger func(string, ...any)) *Topology {
	if logger == nil {
		logger = func(string, ...any) {}
	}
	return &Topology{
		nodes:  make(map[string]*Node),
		chaos:  c,
		logger: logger,
	}
}

// AddNode registers a node and wires its peer set: every other registered
// node becomes a peer, with default thresholds.
func (t *Topology) AddNode(id string, h Handler, strategy lb.Strategy, pol retry.Policy, seed int64) *Node {
	t.mu.Lock()
	defer t.mu.Unlock()
	n := &Node{
		ID:       id,
		tracker:  make(map[string]*peer.Tracker),
		handler:  h,
		strategy: strategy,
		policy:   pol,
		rng:      rand.New(rand.NewSource(seed)),
	}
	t.nodes[id] = n
	// link symmetrically with all existing nodes
	for otherID, other := range t.nodes {
		if otherID == id {
			continue
		}
		n.tracker[otherID] = peer.NewTracker(peer.Endpoint{ID: otherID, Service: "echo", Address: otherID}, peer.DefaultThresholds())
		other.tracker[id] = peer.NewTracker(peer.Endpoint{ID: id, Service: "echo", Address: id}, peer.DefaultThresholds())
	}
	return n
}

// NodeIDs returns the node IDs in sorted order.
func (t *Topology) NodeIDs() []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]string, 0, len(t.nodes))
	for id := range t.nodes {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// Node returns a node by id.
func (t *Topology) Node(id string) *Node {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.nodes[id]
}

// Probe simulates the active health checker probing a single peer.
// The chaos injector decides whether the probe succeeds.
func (t *Topology) Probe(ctx context.Context, src, dst string) error {
	t.mu.RLock()
	srcNode, dstNode := t.nodes[src], t.nodes[dst]
	t.mu.RUnlock()
	if srcNode == nil || dstNode == nil {
		return fmt.Errorf("topology: unknown node %s or %s", src, dst)
	}
	if err := t.chaos.Deliver(ctx, src, dst); err != nil {
		srcNode.tracker[dst].RecordFailure(time.Now())
		return err
	}
	srcNode.tracker[dst].RecordSuccess(time.Now())
	return nil
}

// HealthSweep probes every peer from every node, in parallel. Returns the
// number of (src, dst) pairs that were probed.
func (t *Topology) HealthSweep(ctx context.Context) int {
	t.mu.RLock()
	ids := make([]string, 0, len(t.nodes))
	for id := range t.nodes {
		ids = append(ids, id)
	}
	t.mu.RUnlock()
	var wg sync.WaitGroup
	var count int64
	for _, src := range ids {
		for _, dst := range ids {
			if src == dst {
				continue
			}
			wg.Add(1)
			go func(s, d string) {
				defer wg.Done()
				pctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
				defer cancel()
				_ = t.Probe(pctx, s, d)
				atomic.AddInt64(&count, 1)
			}(src, dst)
		}
	}
	wg.Wait()
	return int(count)
}

// RPCOutcome captures the result of a single end-to-end RPC.
//
// PeerHealthyAtPick is true when the LB observed the chosen peer as healthy
// at the moment of selection. The LB enforces this invariant internally;
// callers can use the field for double-checking in tests.
type RPCOutcome struct {
	From              string
	Service           string
	Attempts          int
	PeerPicked        string
	PeerHealthyAtPick bool
	Err               error
}

// Call is the application-level RPC: the source node uses its LB to pick a
// peer from the destination service, then attempts delivery with retry.
//
// Returns an RPCOutcome with the recorded peer choice for assertions.
func (t *Topology) Call(ctx context.Context, fromID, targetService, payload string) RPCOutcome {
	t.mu.RLock()
	from := t.nodes[fromID]
	t.mu.RUnlock()
	if from == nil {
		return RPCOutcome{From: fromID, Service: targetService, Err: fmt.Errorf("topology: unknown source %s", fromID)}
	}
	pool := from.peerPool()
	exclude := map[string]struct{}{}
	var lastPick string
	var pickedHealthy bool
	// Use a per-call rand sourced under the node mutex so concurrent Call
	// goroutines do not race on the shared rng.
	from.rngMu.Lock()
	seed := from.rng.Int63()
	from.rngMu.Unlock()
	localRng := rand.New(rand.NewSource(seed))
	attempts, err := retry.Do(ctx, from.policy, true, localRng, func(ctx context.Context, attempt int) error {
		picked, perr := from.strategy.Pick(pool, exclude)
		if perr != nil {
			return status.Error(codes.Unavailable, perr.Error())
		}
		lastPick = picked.Endpoint().ID
		// The LB returns only peers it observed as healthy at filter time.
		// We carry that fact forward; reading Health() again here would
		// race against the health checker.
		pickedHealthy = true
		picked.Acquire()
		defer picked.Release()
		callErr := t.deliver(ctx, from.ID, lastPick, payload)
		if callErr != nil {
			picked.RecordFailure(time.Now())
			// exclude this peer from the next retry so we hit a different one
			exclude[lastPick] = struct{}{}
			return callErr
		}
		picked.RecordSuccess(time.Now())
		return nil
	})
	return RPCOutcome{
		From:              fromID,
		Service:           targetService,
		Attempts:          attempts,
		PeerPicked:        lastPick,
		PeerHealthyAtPick: pickedHealthy,
		Err:               err,
	}
}

// deliver invokes the destination's handler if the chaos injector allows.
func (t *Topology) deliver(ctx context.Context, src, dst, payload string) error {
	if err := t.chaos.Deliver(ctx, src, dst); err != nil {
		return status.Error(codes.Unavailable, err.Error())
	}
	dstNode := t.Node(dst)
	if dstNode == nil {
		return status.Error(codes.NotFound, "unknown destination")
	}
	resp, herr := dstNode.handler(ctx, src, dst, payload)
	if herr != nil {
		return herr
	}
	_ = resp
	return nil
}

// peerPool returns the slice of trackers for this node's view of all peers.
func (n *Node) peerPool() []*peer.Tracker {
	out := make([]*peer.Tracker, 0, len(n.tracker))
	for _, t := range n.tracker {
		out = append(out, t)
	}
	// stable order for deterministic round-robin
	sort.Slice(out, func(i, j int) bool { return out[i].Endpoint().ID < out[j].Endpoint().ID })
	return out
}

// PeerHealth returns the health view this node has of dst, or HealthUnknown
// if dst is not a peer.
func (n *Node) PeerHealth(dst string) peer.Health {
	t, ok := n.tracker[dst]
	if !ok {
		return peer.HealthUnknown
	}
	return t.Health()
}

// ConvergedTo returns true when every healthy node's view of every peer
// matches the expected map: expected[id] == true means the node should see
// id as healthy.
//
// We do not assert on the view held by nodes that are themselves expected
// to be unhealthy: a downed node cannot probe its peers, and its tracker
// state is therefore meaningless to the rest of the cluster.
func (t *Topology) ConvergedTo(expected map[string]bool) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	for srcID, n := range t.nodes {
		if want, ok := expected[srcID]; ok && !want {
			continue // skip downed nodes' views
		}
		for dstID, want := range expected {
			if srcID == dstID {
				continue
			}
			got := n.PeerHealth(dstID) == peer.HealthHealthy
			if got != want {
				return false
			}
		}
	}
	return true
}

// WaitForConvergence drives health sweeps until ConvergedTo returns true or
// the deadline is reached. Returns elapsed time and a final-status indicator.
func (t *Topology) WaitForConvergence(ctx context.Context, expected map[string]bool, sweepInterval, deadline time.Duration) (time.Duration, bool) {
	start := time.Now()
	d := time.NewTimer(deadline)
	defer d.Stop()
	for {
		t.HealthSweep(ctx)
		if t.ConvergedTo(expected) {
			return time.Since(start), true
		}
		select {
		case <-d.C:
			return time.Since(start), false
		case <-time.After(sweepInterval):
		}
	}
}

// ErrCancelled wraps context cancellation so callers can recognise it.
var ErrCancelled = errors.New("topology: cancelled")
