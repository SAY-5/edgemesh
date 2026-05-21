package topology

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/SAY-5/edgemesh/internal/chaos"
	"github.com/SAY-5/edgemesh/internal/lb"
	"github.com/SAY-5/edgemesh/internal/peer"
	"github.com/SAY-5/edgemesh/internal/retry"
)

func newSmall(t *testing.T, n int) (*Topology, *chaos.FaultInjector) {
	t.Helper()
	c := chaos.New(chaos.Settings{Seed: 1})
	top := New(c, nil)
	pol := retry.Policy{MaxAttempts: 3, Base: time.Microsecond, Multiplier: 2, Max: time.Millisecond, JitterFrac: 0.1}
	handler := func(_ context.Context, _, _, payload string) (string, error) {
		return payload, nil
	}
	for i := 0; i < n; i++ {
		top.AddNode(fmt.Sprintf("node-%d", i), handler, lb.NewRoundRobin(), pol, int64(i+1))
	}
	return top, c
}

func TestCallSucceedsWithoutFaults(t *testing.T) {
	top, _ := newSmall(t, 4)
	top.HealthSweep(context.Background())
	out := top.Call(context.Background(), "node-0", "echo", "hello")
	if out.Err != nil {
		t.Fatalf("call: %v", out.Err)
	}
	if out.PeerPicked == "" {
		t.Fatal("no peer picked")
	}
	if !out.PeerHealthyAtPick {
		t.Fatal("picked peer was not healthy at selection time")
	}
}

func TestCallSkipsUnhealthyPeer(t *testing.T) {
	top, c := newSmall(t, 3)
	// Mark node-1 down: every probe to it will fail, every call to it will fail.
	c.SetNodeDown("node-1", true)
	// Drive convergence
	for i := 0; i < 6; i++ {
		top.HealthSweep(context.Background())
	}
	// node-0's view of node-1 should now be unhealthy
	src := top.Node("node-0")
	if src.PeerHealth("node-1") != peer.HealthUnhealthy {
		t.Fatalf("node-1 should be unhealthy from node-0's view: %s", src.PeerHealth("node-1"))
	}
	// Issuing 30 calls from node-0 should never pick node-1
	for i := 0; i < 30; i++ {
		out := top.Call(context.Background(), "node-0", "echo", "x")
		if out.PeerPicked == "node-1" {
			t.Fatalf("LB picked an unhealthy peer (call %d)", i)
		}
	}
}

func TestCallEventuallySucceedsAfterPartialFailure(t *testing.T) {
	top, c := newSmall(t, 4)
	c.Partition("node-0", "node-1", true)
	c.Partition("node-0", "node-2", true)
	// node-0 can still reach node-3
	for i := 0; i < 6; i++ {
		top.HealthSweep(context.Background())
	}
	out := top.Call(context.Background(), "node-0", "echo", "x")
	if out.Err != nil {
		t.Fatalf("expected success via node-3: %v", out.Err)
	}
	if out.PeerPicked != "node-3" {
		t.Fatalf("expected pick=node-3, got %s", out.PeerPicked)
	}
}

func TestConvergence(t *testing.T) {
	top, c := newSmall(t, 5)
	// Initial: everyone healthy expected
	expected := map[string]bool{
		"node-0": true,
		"node-1": true,
		"node-2": true,
		"node-3": true,
		"node-4": true,
	}
	elapsed, ok := top.WaitForConvergence(context.Background(), expected, 2*time.Millisecond, time.Second)
	if !ok {
		t.Fatalf("initial convergence failed after %v", elapsed)
	}
	// Take node-2 down
	c.SetNodeDown("node-2", true)
	expected["node-2"] = false
	elapsed, ok = top.WaitForConvergence(context.Background(), expected, 2*time.Millisecond, 2*time.Second)
	if !ok {
		t.Fatalf("convergence after node-2 down failed after %v", elapsed)
	}
}

func TestConcurrentCalls(t *testing.T) {
	top, _ := newSmall(t, 6)
	top.HealthSweep(context.Background())
	var wg sync.WaitGroup
	var errs int64
	const N = 500
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			out := top.Call(context.Background(), fmt.Sprintf("node-%d", i%6), "echo", "x")
			if out.Err != nil {
				errs++
			}
		}(i)
	}
	wg.Wait()
	if errs > 0 {
		t.Fatalf("expected zero errors under no faults, got %d", errs)
	}
}
