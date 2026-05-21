package peer

import (
	"context"
	"sync"
	"time"
)

// Prober probes a single peer. Implementations return nil on success and a
// non-nil error on any failure (transport error, unhealthy status, etc.).
type Prober interface {
	Probe(ctx context.Context, ep Endpoint) error
}

// ProberFunc adapts a function to the Prober interface.
type ProberFunc func(ctx context.Context, ep Endpoint) error

func (f ProberFunc) Probe(ctx context.Context, ep Endpoint) error {
	return f(ctx, ep)
}

// HealthChecker periodically probes a set of trackers.
//
// The checker is intentionally simple: each call to Tick runs one round of
// probes (all in parallel) and updates each tracker's state machine. Real
// deployments call Run which schedules Tick on the configured interval.
type HealthChecker struct {
	prober   Prober
	interval time.Duration
	timeout  time.Duration

	mu       sync.Mutex
	trackers []*Tracker
}

// NewHealthChecker constructs a HealthChecker. Interval defaults to 1s and
// timeout to 200ms when zero is supplied.
func NewHealthChecker(p Prober, interval, timeout time.Duration) *HealthChecker {
	if interval <= 0 {
		interval = time.Second
	}
	if timeout <= 0 {
		timeout = 200 * time.Millisecond
	}
	return &HealthChecker{prober: p, interval: interval, timeout: timeout}
}

// Register adds a tracker to the checker. Safe for concurrent use.
func (h *HealthChecker) Register(t *Tracker) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.trackers = append(h.trackers, t)
}

// Trackers returns the registered trackers in registration order.
func (h *HealthChecker) Trackers() []*Tracker {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]*Tracker, len(h.trackers))
	copy(out, h.trackers)
	return out
}

// Tick runs one probe round and returns the count of healthy peers afterwards.
func (h *HealthChecker) Tick(ctx context.Context, now time.Time) int {
	trackers := h.Trackers()
	var wg sync.WaitGroup
	wg.Add(len(trackers))
	for _, t := range trackers {
		go func(t *Tracker) {
			defer wg.Done()
			pctx, cancel := context.WithTimeout(ctx, h.timeout)
			defer cancel()
			if err := h.prober.Probe(pctx, t.Endpoint()); err != nil {
				t.RecordFailure(now)
				return
			}
			t.RecordSuccess(now)
		}(t)
	}
	wg.Wait()

	healthy := 0
	for _, t := range trackers {
		if t.Health() == HealthHealthy {
			healthy++
		}
	}
	return healthy
}

// Run executes Tick on the configured interval until ctx is cancelled.
func (h *HealthChecker) Run(ctx context.Context) {
	tk := time.NewTicker(h.interval)
	defer tk.Stop()
	// Probe immediately so trackers leave HealthUnknown promptly.
	h.Tick(ctx, time.Now())
	for {
		select {
		case <-ctx.Done():
			return
		case t := <-tk.C:
			h.Tick(ctx, t)
		}
	}
}
