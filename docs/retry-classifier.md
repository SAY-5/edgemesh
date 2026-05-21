# Retry Classifier and Backoff

The retry package implements two things:

1. A pure classifier that maps a gRPC status code to a retry policy
2. An exponential-backoff executor that runs an RPC up to `MaxAttempts`
   times, sleeping between attempts

## Classifier rules

| gRPC code                  | Retry? | Rationale                                  |
|----------------------------|--------|--------------------------------------------|
| `OK`                       | No     | success                                    |
| `UNAVAILABLE`              | Yes    | likely transient, peer may have flapped    |
| `DEADLINE_EXCEEDED`        | Yes    | likely transient                           |
| `ABORTED`                  | Yes    | typically optimistic-concurrency retry     |
| `INVALID_ARGUMENT`         | No     | will not change                            |
| `NOT_FOUND`                | No     | will not change                            |
| `PERMISSION_DENIED`        | No     | will not change                            |
| `UNAUTHENTICATED`          | No     | will not change                            |
| `ALREADY_EXISTS`           | No     | will not change                            |
| `FAILED_PRECONDITION`      | No     | caller bug                                 |
| `OUT_OF_RANGE`             | No     | caller bug                                 |
| `UNIMPLEMENTED`            | No     | will not change                            |
| `INTERNAL`, `DATA_LOSS`    | No     | by default: surface to caller              |
| transport error (non-gRPC) | Yes    | TCP RST, connection refused, dial timeout  |

The retry helper takes an additional boolean `idempotent`: even if the
classifier says "retry", a method marked non-idempotent in YAML never
retries. This prevents accidentally double-charging a payment or
double-creating a resource.

## Backoff

```
sleep(attempt) = min(Base * Multiplier^attempt, Max) * (1 + uniform(-J, +J))
```

Defaults: `Base = 50ms`, `Multiplier = 4`, `Max = 1s`, `J = 0.2`.

That produces 50ms, 200ms, 800ms with +/- 20% jitter. The jitter is applied
per attempt, not as a single random seed, so two callers retrying at the
same instant will sleep different amounts.

## Why this exists

The vanilla gRPC client retries via service-config; that is too coarse for
a sidecar because:

- It cannot consult the health tracker (it retries blindly).
- It does not exclude the peer that just failed; without that, a retry
  often goes to the same broken peer.

The sidecar's retry helper integrates with the LB: between attempts it adds
the failed peer to an exclusion set, so the next pick is guaranteed to be
a different healthy peer (when one exists).
