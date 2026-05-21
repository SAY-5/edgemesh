// Package bench exercises the sidecar data path at high concurrency to
// produce the headline throughput / latency numbers in the README.
//
// The benchmarks intentionally run against the in-process topology (no real
// sockets) so they isolate the LB / retry / health overhead from kernel
// networking.
package bench

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/SAY-5/edgemesh/internal/chaos"
	"github.com/SAY-5/edgemesh/internal/lb"
	"github.com/SAY-5/edgemesh/internal/retry"
	"github.com/SAY-5/edgemesh/internal/topology"
)

func newBenchTopology(b *testing.B, n int) *topology.Topology {
	b.Helper()
	c := chaos.New(chaos.Settings{Seed: 1})
	top := topology.New(c, nil)
	pol := retry.Policy{MaxAttempts: 1, Base: time.Microsecond, Multiplier: 2, Max: time.Millisecond}
	handler := func(_ context.Context, _, _, payload string) (string, error) { return payload, nil }
	for i := 0; i < n; i++ {
		top.AddNode(fmt.Sprintf("node-%02d", i), handler, lb.NewRoundRobin(), pol, int64(i+1))
	}
	top.HealthSweep(context.Background())
	return top
}

func BenchmarkCallSteadyState(b *testing.B) {
	top := newBenchTopology(b, 12)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			out := top.Call(context.Background(), "node-00", "echo", "x")
			if out.Err != nil {
				b.Fatal(out.Err)
			}
		}
	})
}

func BenchmarkCallSinglePeer(b *testing.B) {
	top := newBenchTopology(b, 4)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		out := top.Call(context.Background(), "node-00", "echo", "x")
		if out.Err != nil {
			b.Fatal(out.Err)
		}
	}
}

func BenchmarkHealthSweep(b *testing.B) {
	top := newBenchTopology(b, 12)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		top.HealthSweep(context.Background())
	}
}

// BenchmarkCallChaos runs against a topology with random ~5% drops; this is
// the figure that maps to the "under chaos" column in the README.
func BenchmarkCallChaos(b *testing.B) {
	c := chaos.New(chaos.Settings{Seed: 1, DropProb: 0.05})
	top := topology.New(c, nil)
	pol := retry.Policy{MaxAttempts: 3, Base: 100 * time.Microsecond, Multiplier: 2, Max: time.Millisecond}
	handler := func(_ context.Context, _, _, payload string) (string, error) { return payload, nil }
	for i := 0; i < 12; i++ {
		top.AddNode(fmt.Sprintf("node-%02d", i), handler, lb.NewRoundRobin(), pol, int64(i+1))
	}
	top.HealthSweep(context.Background())
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = top.Call(context.Background(), "node-00", "echo", "x")
		}
	})
}

// BenchmarkCallMixedConcurrent runs a multi-source workload to test fairness.
func BenchmarkCallMixedConcurrent(b *testing.B) {
	top := newBenchTopology(b, 12)
	b.ResetTimer()
	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			out := top.Call(context.Background(), fmt.Sprintf("node-%02d", i%12), "echo", "x")
			if out.Err != nil {
				b.Error(out.Err)
			}
		}(i)
	}
	wg.Wait()
}
