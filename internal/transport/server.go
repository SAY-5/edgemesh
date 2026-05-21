package transport

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/SAY-5/edgemesh/internal/config"
	"github.com/SAY-5/edgemesh/internal/lb"
	"github.com/SAY-5/edgemesh/internal/peer"
	meshpb "github.com/SAY-5/edgemesh/proto/edgemesh"
)

// Sidecar is the gRPC-side of the mesh sidecar. It owns the peer trackers,
// the health checker (already wired by the caller), and the LB strategy.
type Sidecar struct {
	meshpb.UnimplementedMeshServer

	cfg      *config.Config
	hc       *peer.HealthChecker
	strategy lb.Strategy

	mu    sync.RWMutex
	drain bool
}

// New constructs the gRPC service implementation.
func New(cfg *config.Config, hc *peer.HealthChecker, strategy lb.Strategy) *Sidecar {
	return &Sidecar{cfg: cfg, hc: hc, strategy: strategy}
}

// Register attaches the Sidecar and the standard health service to a gRPC
// server.
func (s *Sidecar) Register(server *grpc.Server) {
	meshpb.RegisterMeshServer(server, s)
	healthpb.RegisterHealthServer(server, healthService{s: s})
}

// ListPeers returns the current state of every tracked peer.
func (s *Sidecar) ListPeers(_ context.Context, _ *meshpb.ListPeersRequest) (*meshpb.ListPeersResponse, error) {
	out := &meshpb.ListPeersResponse{}
	for _, t := range s.hc.Trackers() {
		out.Peers = append(out.Peers, snapshotToProto(t.Snapshot()))
	}
	return out, nil
}

// GetPeer returns the state of a single peer by ID.
func (s *Sidecar) GetPeer(_ context.Context, req *meshpb.GetPeerRequest) (*meshpb.PeerStatus, error) {
	for _, t := range s.hc.Trackers() {
		if t.Endpoint().ID == req.GetPeerId() {
			return snapshotToProto(t.Snapshot()), nil
		}
	}
	return nil, status.Errorf(codes.NotFound, "peer %q not found", req.GetPeerId())
}

// Drain marks the sidecar as draining: subsequent calls report NOT_SERVING
// on the health endpoint.
func (s *Sidecar) Drain(_ context.Context, req *meshpb.DrainRequest) (*meshpb.DrainResponse, error) {
	s.mu.Lock()
	s.drain = true
	s.mu.Unlock()
	if req.GetGraceSeconds() > 0 {
		// best-effort: nothing in this codebase actively waits, but the
		// flag is observable to load balancers via the health endpoint.
		_ = time.Duration(req.GetGraceSeconds()) * time.Second
	}
	return &meshpb.DrainResponse{Ok: true, Message: "draining"}, nil
}

// Forward picks a healthy peer using the configured strategy. The data plane
// would then dial that peer with a sidecar-to-sidecar gRPC call; in this
// build we surface the picked peer in the response so meshctl can drive
// integration tests against it.
func (s *Sidecar) Forward(_ context.Context, req *meshpb.ForwardRequest) (*meshpb.ForwardResponse, error) {
	pool := s.hc.Trackers()
	picked, err := s.strategy.Pick(pool, nil)
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "no healthy peer: %v", err)
	}
	return &meshpb.ForwardResponse{
		Payload:  req.GetPayload(),
		Attempts: 1,
		PeerId:   picked.Endpoint().ID,
	}, nil
}

// IsDraining reports whether the sidecar has been signalled to drain.
func (s *Sidecar) IsDraining() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.drain
}

func snapshotToProto(s peer.Snapshot) *meshpb.PeerStatus {
	var h meshpb.Health
	switch s.Health {
	case peer.HealthHealthy:
		h = meshpb.Health_HEALTH_HEALTHY
	case peer.HealthUnhealthy:
		h = meshpb.Health_HEALTH_UNHEALTHY
	default:
		h = meshpb.Health_HEALTH_UNKNOWN
	}
	return &meshpb.PeerStatus{
		PeerId:               s.Endpoint.ID,
		Service:              s.Endpoint.Service,
		Address:              s.Endpoint.Address,
		Health:               h,
		LastProbeUnixNano:    s.LastProbe.UnixNano(),
		ConsecutiveSuccesses: s.ConsecutiveSuccesses,
		ConsecutiveFailures:  s.ConsecutiveFailures,
		InFlight:             uint32(s.InFlight),
	}
}

// healthService bridges the standard gRPC health protocol to the sidecar's
// drain state.
type healthService struct {
	healthpb.UnimplementedHealthServer
	s *Sidecar
}

func (h healthService) Check(_ context.Context, _ *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	if h.s.IsDraining() {
		return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_NOT_SERVING}, nil
	}
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

// Listen builds a net.Listener from a Listen config. Supports TCP and Unix
// domain sockets.
func Listen(l config.Listen) (net.Listener, error) {
	if l.UnixSocket != "" {
		return net.Listen("unix", l.UnixSocket)
	}
	if l.Address == "" {
		return nil, fmt.Errorf("transport: no listen address configured")
	}
	return net.Listen("tcp", l.Address)
}
