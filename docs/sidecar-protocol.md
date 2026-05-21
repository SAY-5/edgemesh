# Sidecar Protocol

The sidecar exposes a single gRPC service, `edgemesh.v1.Mesh`, defined in
`proto/edgemesh/mesh.proto`. It is intended to be reached over loopback or a
Unix domain socket by the application that shares its pod.

## RPCs

### ListPeers

Returns every tracked peer with its current health, last probe timestamp,
consecutive success/failure counts, and in-flight RPC count.

Use this for dashboards and `meshctl status`.

### GetPeer

Returns the same `PeerStatus` for a single peer id.

### Drain

Marks the sidecar as draining. After this call:

- The standard `grpc.health.v1.Health/Check` endpoint reports `NOT_SERVING`.
- The sidecar continues serving in-flight RPCs.

Operators send Drain before terminating a pod so other sidecars route around
it within `health.unhealthy_to_healthy * health.interval`.

### Forward

Picks a healthy peer using the configured load-balancing strategy and returns
its id. Returns `UNAVAILABLE` when no healthy peer is available.

This is the primitive used by the application client library; in this build
the actual sidecar-to-sidecar forwarding is exercised by the topology
simulator rather than over the wire.

## Co-located health protocol

The sidecar additionally serves `grpc.health.v1.Health` on the same socket
so external orchestrators (Kubernetes liveness probes, service meshes, load
balancers) can use the standard health protocol without any custom client.

## Concurrency model

- The peer tracker map is read-mostly; writes happen on Register at startup.
- Per-peer state (counters, in-flight) is protected by a mutex / atomics.
- The load balancer is safe for concurrent use; round-robin uses an atomic
  cursor.
- The retry helper accepts a `*rand.Rand`, so the caller is responsible for
  serialising or per-goroutine cloning.
