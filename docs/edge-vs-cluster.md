# Edge vs Cluster

A service mesh inside a single Kubernetes cluster and a service mesh on a
network of edge sites have different failure modes. EdgeMesh is tuned for
the edge case. This document spells out the difference.

## What "edge" means here

By edge we mean any deployment where:

- Inter-node latency is highly variable (5ms - 500ms across links)
- Partitions are common and asymmetric (a -> b can fail while b -> a works)
- A whole node can drop off the network for minutes and come back
- The mesh has tens, not thousands, of peers

This describes typical multi-region SaaS as well as IoT gateways, retail
branch networks, and CDN PoPs.

## Defaults

| Parameter            | Cluster (Envoy default) | EdgeMesh default |
|----------------------|-------------------------|------------------|
| Health probe period  | 5s                      | 1s               |
| Probe timeout        | 1s                      | 200ms            |
| Unhealthy threshold  | 5 consecutive failures  | 3                |
| Recovery threshold   | 1 consecutive success   | 2                |
| Retry budget         | 1                       | 3                |
| Retry initial sleep  | 25ms                    | 50ms             |

The asymmetry between unhealthy and recovery thresholds matters more on
the edge: a flap should not bring a peer back into rotation prematurely.

## Connectivity model

In a cluster, the network is approximately reliable; partitions are rare.
The Envoy / Istio default is to assume the wire is fast and retries are
cheap.

On the edge, partitions are the norm. The default is:

- Health checks are aggressive (1s, 200ms timeout)
- The LB filters unhealthy peers strictly
- Retries always exclude the previously-failing peer
- Idempotency is encoded per-method in YAML so unsafe methods never retry

## Observability

We do not assume a Prometheus or Jaeger collector is reachable. The
sidecar exposes its state on its own gRPC API (`ListPeers`, `GetPeer`) so
`meshctl status` can be run from inside the node itself. An OTel exporter
hook is wired but no collector ships by default.
