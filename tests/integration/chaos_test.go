package integration

import (
	"context"
	"os"
	"strconv"
	"testing"
)

// Test_12NodeChaos is the load-bearing integration test. It is gated by
// RUN_CHAOS=1 to keep the default `go test ./...` fast; CI sets the env var
// in the test-integration job.
func Test_12NodeChaos(t *testing.T) {
	if os.Getenv("RUN_CHAOS") == "" {
		t.Skip("set RUN_CHAOS=1 to run the 12-node chaos suite")
	}
	settings := DefaultRunSettings()
	if v := os.Getenv("CHAOS_SCENARIOS"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			t.Fatalf("bad CHAOS_SCENARIOS: %v", err)
		}
		settings.Scenarios = n
	}
	res, err := Run(context.Background(), settings)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("chaos result: %+v", res)

	// Safety: the load balancer never selects an unhealthy peer.
	if res.LBInvariantHits != 0 {
		t.Fatalf("LB selected an unhealthy peer %d times", res.LBInvariantHits)
	}
	// Correctness: every RPC is accounted for as success or classified failure.
	if res.UnaccountedRPCs != 0 {
		t.Fatalf("%d RPCs unaccounted for", res.UnaccountedRPCs)
	}
	// Liveness: at least most scenarios should converge within the budget.
	if res.ScenariosPassed < int(float64(res.Scenarios)*0.9) {
		t.Fatalf("only %d/%d scenarios passed (<90%%)", res.ScenariosPassed, res.Scenarios)
	}
}
