package lb

import (
	"testing"
	"time"

	"github.com/SAY-5/edgemesh/internal/peer"
)

func makePool(ids ...string) []*peer.Tracker {
	out := make([]*peer.Tracker, 0, len(ids))
	for _, id := range ids {
		t := peer.NewTracker(peer.Endpoint{ID: id, Service: "svc", Address: "addr-" + id}, peer.DefaultThresholds())
		t.RecordSuccess(time.Now())
		out = append(out, t)
	}
	return out
}

func TestRoundRobinHealthyOnly(t *testing.T) {
	pool := makePool("a", "b", "c")
	// Mark b as unhealthy via the state machine (1 failure threshold).
	bad := peer.NewTracker(peer.Endpoint{ID: "x"}, peer.Thresholds{HealthyToUnhealthy: 1, UnhealthyToHealthy: 1})
	bad.RecordFailure(time.Now())
	pool = append(pool, bad)

	rr := NewRoundRobin()
	picks := map[string]int{}
	for i := 0; i < 30; i++ {
		t.Helper()
		p, err := rr.Pick(pool, nil)
		if err != nil {
			t.Fatalf("pick failed: %v", err)
		}
		picks[p.Endpoint().ID]++
	}
	if picks["x"] != 0 {
		t.Fatalf("unhealthy peer was selected: %+v", picks)
	}
	if picks["a"] == 0 || picks["b"] == 0 || picks["c"] == 0 {
		t.Fatalf("not all healthy peers received traffic: %+v", picks)
	}
	// Distribution is round-robin across 3 peers over 30 picks: each gets 10.
	if picks["a"] != 10 || picks["b"] != 10 || picks["c"] != 10 {
		t.Fatalf("uneven distribution: %+v", picks)
	}
}

func TestRoundRobinExclude(t *testing.T) {
	pool := makePool("a", "b", "c")
	rr := NewRoundRobin()
	exclude := map[string]struct{}{"b": {}}
	for i := 0; i < 10; i++ {
		p, err := rr.Pick(pool, exclude)
		if err != nil {
			t.Fatalf("pick: %v", err)
		}
		if p.Endpoint().ID == "b" {
			t.Fatalf("excluded peer was picked")
		}
	}
}

func TestRoundRobinNoHealthy(t *testing.T) {
	bad := peer.NewTracker(peer.Endpoint{ID: "x"}, peer.Thresholds{HealthyToUnhealthy: 1})
	bad.RecordFailure(time.Now())
	rr := NewRoundRobin()
	if _, err := rr.Pick([]*peer.Tracker{bad}, nil); err == nil {
		t.Fatalf("expected ErrNoHealthyPeers")
	}
}

func TestLeastPendingPicksMinimum(t *testing.T) {
	pool := makePool("a", "b", "c")
	pool[0].Acquire() // a=1
	pool[0].Acquire() // a=2
	pool[1].Acquire() // b=1
	// c=0
	lp := NewLeastPending()
	got, err := lp.Pick(pool, nil)
	if err != nil {
		t.Fatalf("pick: %v", err)
	}
	if got.Endpoint().ID != "c" {
		t.Fatalf("expected c (load 0), got %s", got.Endpoint().ID)
	}
}

func TestLeastPendingSkipsExcludedAndUnhealthy(t *testing.T) {
	pool := makePool("a", "b", "c")
	bad := peer.NewTracker(peer.Endpoint{ID: "x"}, peer.Thresholds{HealthyToUnhealthy: 1})
	bad.RecordFailure(time.Now())
	pool = append(pool, bad)
	pool[0].Acquire() // a=1
	// b=0 (would be picked) but excluded
	pool[2].Acquire() // c=1
	lp := NewLeastPending()
	got, err := lp.Pick(pool, map[string]struct{}{"b": {}})
	if err != nil {
		t.Fatalf("pick: %v", err)
	}
	if got.Endpoint().ID != "a" && got.Endpoint().ID != "c" {
		t.Fatalf("unexpected pick %s", got.Endpoint().ID)
	}
	if got.Endpoint().ID == "x" {
		t.Fatalf("picked unhealthy x")
	}
}

func TestFromName(t *testing.T) {
	if NewRoundRobin().Name() != "round-robin" {
		t.Fatal("rr name wrong")
	}
	if NewLeastPending().Name() != "least-pending" {
		t.Fatal("lp name wrong")
	}
	if FromName("least-pending").Name() != "least-pending" {
		t.Fatal("FromName lp wrong")
	}
	if FromName("anything").Name() != "round-robin" {
		t.Fatal("FromName default wrong")
	}
}
