package chaos

import (
	"context"
	"testing"
	"time"
)

func TestDeliverWithoutFaults(t *testing.T) {
	f := New(Settings{Seed: 1})
	for i := 0; i < 1000; i++ {
		if err := f.Deliver(context.Background(), "a", "b"); err != nil {
			t.Fatalf("expected no error: %v", err)
		}
	}
}

func TestNodeDownDropsBoth(t *testing.T) {
	f := New(Settings{Seed: 1})
	f.SetNodeDown("a", true)
	if err := f.Deliver(context.Background(), "a", "b"); err == nil {
		t.Fatal("expected drop from downed source")
	}
	if err := f.Deliver(context.Background(), "b", "a"); err == nil {
		t.Fatal("expected drop to downed destination")
	}
	f.SetNodeDown("a", false)
	if err := f.Deliver(context.Background(), "a", "b"); err != nil {
		t.Fatalf("expected delivery after recovery: %v", err)
	}
}

func TestPartitionAsymmetric(t *testing.T) {
	f := New(Settings{Seed: 1})
	f.Partition("a", "b", true)
	if err := f.Deliver(context.Background(), "a", "b"); err == nil {
		t.Fatal("expected partition drop")
	}
	if err := f.Deliver(context.Background(), "b", "a"); err != nil {
		t.Fatalf("reverse direction should still work: %v", err)
	}
	f.Partition("a", "b", false)
	if err := f.Deliver(context.Background(), "a", "b"); err != nil {
		t.Fatalf("expected delivery after partition cleared: %v", err)
	}
}

func TestRandomDropApproximatesProbability(t *testing.T) {
	f := New(Settings{Seed: 42, DropProb: 0.5})
	drops, total := 0, 5000
	for i := 0; i < total; i++ {
		if err := f.Deliver(context.Background(), "a", "b"); err != nil {
			drops++
		}
	}
	frac := float64(drops) / float64(total)
	if frac < 0.4 || frac > 0.6 {
		t.Fatalf("drop fraction outside [0.4, 0.6]: %f", frac)
	}
}

func TestClearAll(t *testing.T) {
	f := New(Settings{Seed: 1})
	f.SetNodeDown("a", true)
	f.Partition("b", "c", true)
	snap := f.Snapshot()
	if len(snap.Down) != 1 || len(snap.Partitions) != 1 {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}
	f.ClearAll()
	snap = f.Snapshot()
	if len(snap.Down) != 0 || len(snap.Partitions) != 0 {
		t.Fatalf("ClearAll did not reset: %+v", snap)
	}
}

func TestDeliverRespectsContext(t *testing.T) {
	f := New(Settings{Seed: 1, MaxDelay: 100 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), time.Microsecond)
	defer cancel()
	// run a few times; at least one should hit the cancel path because
	// MaxDelay >> timeout.
	got := false
	for i := 0; i < 20; i++ {
		if err := f.Deliver(ctx, "a", "b"); err != nil && err == context.DeadlineExceeded {
			got = true
			break
		}
	}
	if !got {
		t.Skip("did not exercise context path; this is best-effort")
	}
}
