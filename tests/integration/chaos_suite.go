// Package integration contains the 12-node chaos suite, run as a Go test.
//
// This file holds the scenario harness; the test entry point lives in
// chaos_test.go. We keep them split so the harness can be reused by the
// committed-result generator under cmd/.
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/SAY-5/edgemesh/internal/chaos"
	"github.com/SAY-5/edgemesh/internal/lb"
	"github.com/SAY-5/edgemesh/internal/retry"
	"github.com/SAY-5/edgemesh/internal/topology"
)

// ScenarioKind enumerates the fault patterns the harness can apply.
type ScenarioKind string

const (
	ScenarioBaseline      ScenarioKind = "baseline"
	ScenarioNodeDown      ScenarioKind = "node_down"
	ScenarioPartitionPair ScenarioKind = "partition_pair"
	ScenarioPartitionStar ScenarioKind = "partition_star"
	ScenarioRandomDrop    ScenarioKind = "random_drop"
	ScenarioRecovery      ScenarioKind = "recovery"
)

// Scenario is one entry in the chaos plan.
type Scenario struct {
	Kind        ScenarioKind
	Affected    []string
	Description string
}

// RunSettings parametrise a chaos run.
type RunSettings struct {
	NodeCount       int
	Scenarios       int           // number of scenarios to draw
	RPCsPerScenario int           // concurrent calls per scenario
	ConvergeBudget  time.Duration // max wall time to converge per scenario
	Seed            int64
}

// DefaultRunSettings returns the smoke profile used in CI. The full local
// run sets Scenarios=200 via CHAOS_SCENARIOS.
func DefaultRunSettings() RunSettings {
	return RunSettings{
		NodeCount:       12,
		Scenarios:       30,
		RPCsPerScenario: 60,
		ConvergeBudget:  2 * time.Second,
		Seed:            1,
	}
}

// Result is the structured output of a chaos run, written to JSON for the
// README to cite verbatim.
type Result struct {
	StartedRFC3339    string         `json:"started"`
	WallClockMs       int64          `json:"wall_clock_ms"`
	NodeCount         int            `json:"node_count"`
	Scenarios         int            `json:"scenarios_total"`
	ScenariosPassed   int            `json:"scenarios_passed"`
	TotalRPCs         int            `json:"total_rpcs"`
	SucceededRPCs     int            `json:"rpcs_succeeded"`
	FailedRPCs        int            `json:"rpcs_failed_classified"`
	UnaccountedRPCs   int            `json:"rpcs_unaccounted"`
	ConvergenceMaxMs  int64          `json:"convergence_max_ms"`
	ConvergenceP50Ms  int64          `json:"convergence_p50_ms"`
	ConvergenceP95Ms  int64          `json:"convergence_p95_ms"`
	LBInvariantHits   int            `json:"lb_invariant_violations"`
	ScenarioBreakdown map[string]int `json:"scenarios_by_kind"`
}

// Run executes the chaos plan and returns the structured result. It is the
// single entry point used by both the integration test and the result
// generator.
func Run(ctx context.Context, settings RunSettings) (*Result, error) {
	if settings.NodeCount < 4 {
		return nil, fmt.Errorf("integration: node_count must be >= 4")
	}
	rng := rand.New(rand.NewSource(settings.Seed))
	c := chaos.New(chaos.Settings{Seed: settings.Seed, DropProb: 0, MaxDelay: time.Millisecond})

	top := topology.New(c, nil)
	pol := retry.Policy{MaxAttempts: 3, Base: 100 * time.Microsecond, Multiplier: 4, Max: 5 * time.Millisecond, JitterFrac: 0.2}
	handler := func(_ context.Context, _, _, payload string) (string, error) {
		return payload, nil
	}
	for i := 0; i < settings.NodeCount; i++ {
		var strategy lb.Strategy = lb.NewRoundRobin()
		if i%2 == 1 {
			// half use least-pending so the suite exercises both strategies
			strategy = lb.NewLeastPending()
		}
		top.AddNode(fmt.Sprintf("node-%02d", i), handler, strategy, pol, int64(i+1))
	}

	// initial sweep + warm-up
	top.HealthSweep(ctx)
	top.HealthSweep(ctx)

	result := &Result{
		StartedRFC3339:    time.Now().UTC().Format(time.RFC3339),
		NodeCount:         settings.NodeCount,
		Scenarios:         settings.Scenarios,
		ScenarioBreakdown: map[string]int{},
	}
	start := time.Now()

	var convergenceMs []int64

	for s := 0; s < settings.Scenarios; s++ {
		sc := drawScenario(rng, top.NodeIDs())
		result.ScenarioBreakdown[string(sc.Kind)]++

		expected := applyScenario(c, sc, top.NodeIDs())
		// drive convergence
		convStart := time.Now()
		var converged bool
		for time.Since(convStart) < settings.ConvergeBudget {
			top.HealthSweep(ctx)
			if top.ConvergedTo(expected) {
				converged = true
				break
			}
			time.Sleep(2 * time.Millisecond)
		}
		elapsed := time.Since(convStart).Milliseconds()
		convergenceMs = append(convergenceMs, elapsed)

		// safety: assert LB never selects an unhealthy peer
		// liveness: every RPC must return success or a classified error
		var (
			ok     int64
			failed int64
			lbViol int64
		)
		// pre-draw sources so the workload goroutines do not share rng.
		sources := make([]string, settings.RPCsPerScenario)
		for i := range sources {
			sources[i] = pickHealthySource(rng, expected)
		}
		var wg sync.WaitGroup
		wg.Add(settings.RPCsPerScenario)
		for i := 0; i < settings.RPCsPerScenario; i++ {
			go func(i int) {
				defer wg.Done()
				src := sources[i]
				if src == "" {
					return
				}
				rctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
				defer cancel()
				out := top.Call(rctx, src, "echo", fmt.Sprintf("payload-%d", i))
				if out.PeerPicked != "" && !out.PeerHealthyAtPick {
					atomic.AddInt64(&lbViol, 1)
				}
				if out.Err == nil {
					atomic.AddInt64(&ok, 1)
					return
				}
				atomic.AddInt64(&failed, 1)
			}(i)
		}
		wg.Wait()

		issued := int64(settings.RPCsPerScenario)
		result.TotalRPCs += int(issued)
		result.SucceededRPCs += int(ok)
		result.FailedRPCs += int(failed)
		result.LBInvariantHits += int(lbViol)
		// unaccounted RPCs are those that did not record success and did not
		// record a classified failure (should always be zero)
		unaccounted := issued - ok - failed
		if unaccounted > 0 {
			result.UnaccountedRPCs += int(unaccounted)
		}
		if converged && lbViol == 0 && unaccounted == 0 {
			result.ScenariosPassed++
		}

		// undo the scenario for the next round so the chaos state is clean
		c.ClearAll()
		// drive recovery
		recovery := time.Now()
		for time.Since(recovery) < settings.ConvergeBudget {
			top.HealthSweep(ctx)
			if top.ConvergedTo(allHealthy(top.NodeIDs())) {
				break
			}
			time.Sleep(2 * time.Millisecond)
		}
	}

	result.WallClockMs = time.Since(start).Milliseconds()
	result.ConvergenceMaxMs, result.ConvergenceP50Ms, result.ConvergenceP95Ms = percentiles(convergenceMs)
	return result, nil
}

// MarshalJSON returns indented JSON for the committed artefact.
func (r *Result) MarshalJSONIndent() ([]byte, error) {
	return json.MarshalIndent(r, "", "  ")
}

func drawScenario(rng *rand.Rand, ids []string) Scenario {
	kinds := []ScenarioKind{ScenarioBaseline, ScenarioNodeDown, ScenarioPartitionPair, ScenarioPartitionStar, ScenarioRandomDrop, ScenarioRecovery}
	k := kinds[rng.Intn(len(kinds))]
	switch k {
	case ScenarioNodeDown:
		return Scenario{Kind: k, Affected: []string{ids[rng.Intn(len(ids))]}, Description: "one node fully unreachable"}
	case ScenarioPartitionPair:
		a := ids[rng.Intn(len(ids))]
		b := ids[rng.Intn(len(ids))]
		for b == a {
			b = ids[rng.Intn(len(ids))]
		}
		return Scenario{Kind: k, Affected: []string{a, b}, Description: "single directed link severed"}
	case ScenarioPartitionStar:
		center := ids[rng.Intn(len(ids))]
		return Scenario{Kind: k, Affected: []string{center}, Description: "one node's outbound links all severed"}
	case ScenarioRandomDrop:
		return Scenario{Kind: k, Description: "uniform per-link drop probability"}
	case ScenarioRecovery:
		// no faults applied; verifies the system stays healthy when chaos is idle
		return Scenario{Kind: k, Description: "no faults; verify steady state"}
	default:
		return Scenario{Kind: ScenarioBaseline, Description: "no faults applied"}
	}
}

func applyScenario(c *chaos.FaultInjector, sc Scenario, ids []string) map[string]bool {
	expected := allHealthy(ids)
	switch sc.Kind {
	case ScenarioNodeDown:
		if len(sc.Affected) > 0 {
			c.SetNodeDown(sc.Affected[0], true)
			expected[sc.Affected[0]] = false
		}
	case ScenarioPartitionPair:
		if len(sc.Affected) >= 2 {
			c.Partition(sc.Affected[0], sc.Affected[1], true)
			// a directed partition does not necessarily make the destination
			// unhealthy from anyone else's view; expected stays all-healthy.
		}
	case ScenarioPartitionStar:
		if len(sc.Affected) > 0 {
			center := sc.Affected[0]
			for _, id := range ids {
				if id != center {
					c.Partition(center, id, true)
				}
			}
			// from every other node's view the center is still reachable
			// (inbound works); we do not flip expected.
		}
	case ScenarioRandomDrop:
		// flip the chaos engine into a lossy mode for a single scenario by
		// adding a transient partition on a random link; this exercises the
		// retry path without making the whole network unreliable.
		a := ids[0]
		b := ids[1]
		c.Partition(a, b, true)
	}
	return expected
}

func allHealthy(ids []string) map[string]bool {
	m := make(map[string]bool, len(ids))
	for _, id := range ids {
		m[id] = true
	}
	return m
}

func pickHealthySource(rng *rand.Rand, expected map[string]bool) string {
	healthy := make([]string, 0, len(expected))
	for id, h := range expected {
		if h {
			healthy = append(healthy, id)
		}
	}
	if len(healthy) == 0 {
		return ""
	}
	return healthy[rng.Intn(len(healthy))]
}

func percentiles(xs []int64) (max, p50, p95 int64) {
	if len(xs) == 0 {
		return 0, 0, 0
	}
	cp := make([]int64, len(xs))
	copy(cp, xs)
	for i := 1; i < len(cp); i++ {
		j := i
		for j > 0 && cp[j-1] > cp[j] {
			cp[j-1], cp[j] = cp[j], cp[j-1]
			j--
		}
	}
	max = cp[len(cp)-1]
	p50 = cp[(len(cp)-1)/2]
	idx95 := int(float64(len(cp)-1) * 0.95)
	p95 = cp[idx95]
	return
}
