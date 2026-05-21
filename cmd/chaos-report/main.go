// Command chaos-report runs the 12-node chaos suite and writes the result
// JSON to the path given on the command line. The README's headline result
// is generated from this committed file; it is not re-run in CI.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/SAY-5/edgemesh/tests/integration"
)

func main() {
	out := flag.String("out", "bench/results/chaos-12-node.json", "path for the result JSON")
	scenarios := flag.Int("scenarios", 200, "number of scenarios to draw")
	rpcs := flag.Int("rpcs", 60, "concurrent RPCs per scenario")
	seed := flag.Int64("seed", 1, "rng seed")
	flag.Parse()

	settings := integration.DefaultRunSettings()
	settings.Scenarios = *scenarios
	settings.RPCsPerScenario = *rpcs
	settings.Seed = *seed

	res, err := integration.Run(context.Background(), settings)
	if err != nil {
		log.Fatal(err)
	}
	body, err := res.MarshalJSONIndent()
	if err != nil {
		log.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		log.Fatal(err)
	}
	if err := os.WriteFile(*out, body, 0o600); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("wrote %s (%d bytes)\n", *out, len(body))
	fmt.Printf("scenarios passed: %d/%d\n", res.ScenariosPassed, res.Scenarios)
	fmt.Printf("total rpcs: %d (succ=%d failed=%d unaccounted=%d)\n",
		res.TotalRPCs, res.SucceededRPCs, res.FailedRPCs, res.UnaccountedRPCs)
	fmt.Printf("convergence ms: p50=%d p95=%d max=%d\n",
		res.ConvergenceP50Ms, res.ConvergenceP95Ms, res.ConvergenceMaxMs)
	fmt.Printf("lb invariant violations: %d\n", res.LBInvariantHits)
}
