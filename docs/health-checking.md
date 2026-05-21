# Active Health Checking

Every sidecar maintains a `Tracker` per peer. The tracker tracks:

- An immutable `Endpoint` (id, service, address)
- A `Health` value in `{Unknown, Healthy, Unhealthy}`
- Two counters: consecutive successes, consecutive failures
- The timestamp of the last probe
- An in-flight RPC counter (used by least-pending)

## State machine

```
                      success
            +--------------------------------+
            |                                v
        +--------+                       +--------+
        |Unhealthy|---success * N------->|Healthy |
        +--------+                       +--------+
            ^                                |
            |                                |
            +----failure * M-----------------+

    Unknown -> Healthy   on the first success
    Unknown -> Unhealthy on the first failure
```

Thresholds (default):
- `M = HealthyToUnhealthy = 3`
- `N = UnhealthyToHealthy = 2`

The asymmetry is intentional: we react faster to a peer going bad than to
one coming back. This matches Envoy's outlier-detection guidance.

## Probe protocol

The active prober calls the standard `grpc.health.v1.Health/Check` RPC on
every peer. A response of `SERVING` is a success; everything else (transport
error, `NOT_SERVING`, `UNKNOWN`) is a failure.

Probes run on a configurable interval (default 1s) with a per-call timeout
(default 200ms). Probes from the same sidecar run in parallel goroutines.

## Convergence

The chaos suite asserts that the cluster's view of "which peers are
healthy" matches the actual fault state within a budget. With the default
1s probe interval and 3-failure threshold, a node going down should be
marked unhealthy by every other node within 3 seconds. The committed
benchmark at 12 nodes uses 2ms sweeps and reports a p95 convergence time
of 8ms because the test environment can probe far more aggressively than a
real network would tolerate.
