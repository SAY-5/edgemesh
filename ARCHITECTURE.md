# Architecture

This document gives the picture an engineer needs before reading the code.

## Components

```
                    +-------------------+
   application ---->|  sidecar gRPC API |  (Mesh service, loopback / UDS)
                    +---------+---------+
                              |
                  +-----------+-----------+
                  |                       |
            +-----+------+         +------+------+
            | retry      |         | LB strategy |
            | classifier |         | (RR / LP)   |
            +-----+------+         +------+------+
                  |                       |
                  +-----+----------+------+
                        |          |
                  +-----v----+ +---v-----+
                  | tracker  | | tracker | ... per peer
                  +-----+----+ +----+----+
                        |          |
                  +-----v----------v-----+
                  |  health checker      |  (gRPC Health/Check probes)
                  +-----+----------------+
                        |
                        v  (wire)  +---> peer sidecar
                                   +---> peer sidecar
                                   +---> peer sidecar
```

## Package layout

| Package                | Purpose                                          |
|------------------------|--------------------------------------------------|
| `internal/peer`        | Tracker + health-checker state machine           |
| `internal/lb`          | Round-robin and least-pending strategies         |
| `internal/retry`       | Classifier + exponential backoff                 |
| `internal/transport`   | gRPC server, gRPC-backed prober                  |
| `internal/config`      | YAML loader and validation                       |
| `internal/topology`    | In-process N-node simulator                      |
| `internal/chaos`       | Fault injector (partitions, drops, node-down)    |
| `proto/edgemesh`       | Mesh control-plane proto                         |
| `proto/echo`           | Demo service used by the chaos suite             |
| `cmd/sidecar`          | Sidecar binary                                   |
| `cmd/meshctl`          | Operator CLI                                     |
| `cmd/chaos-report`     | Generates the committed chaos result JSON        |
| `tests/integration`    | 12-node chaos suite                              |
| `bench/`               | Throughput / latency benchmarks                  |
| `k8s/`                 | Kustomize base + dev/stg/prod overlays           |

## Sidecar lifecycle

1. Load YAML from `-config` (or `EDGEMESH_CONFIG`).
2. For each configured peer, create a `Tracker` with the configured
   thresholds. Register all trackers with the health checker.
3. Start the health checker on its own goroutine. The first probe round
   runs immediately so trackers leave `HealthUnknown` promptly.
4. Bind the gRPC listener (TCP or Unix socket).
5. Register the Mesh service and the standard health service.
6. Serve. On SIGINT / SIGTERM call `GracefulStop` with a 5s deadline,
   then `Stop` if it has not finished.

## Health checker state machine

See `docs/health-checking.md`. Summary:

- Default thresholds: 3 failures to mark unhealthy, 2 successes to recover.
- Asymmetric on purpose: react fast to badness, slow to recovery.
- Probe protocol is the standard `grpc.health.v1.Health/Check`.

## Retry classifier

See `docs/retry-classifier.md`. Summary:

- Retry on `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `ABORTED`, non-gRPC errors.
- Do not retry on `INVALID_ARGUMENT`, `NOT_FOUND`, `PERMISSION_DENIED`,
  `UNAUTHENTICATED`, `ALREADY_EXISTS`, `FAILED_PRECONDITION`,
  `OUT_OF_RANGE`, `UNIMPLEMENTED`.
- Always check the YAML `idempotent` flag first; non-idempotent methods
  never retry.

## Load-balancer strategies

- `round-robin`: filter to healthy non-excluded peers, advance an atomic
  cursor mod the eligible-set size.
- `least-pending`: filter to healthy non-excluded peers, choose the one
  with the smallest in-flight count (tie-broken by id).

Both strategies are safe for concurrent use.

## Chaos test methodology

See `docs/chaos-methodology.md`. The 12-node suite uses six scenario kinds
and asserts safety, liveness, and convergence after each.

## Edge vs cluster

See `docs/edge-vs-cluster.md`. The defaults are tuned for the variable-
latency / asymmetric-partition profile typical of edge deployments.
