// Package transport wires the sidecar's gRPC server and the gRPC-backed
// peer prober. The prober uses the standard grpc.health.v1.Health/Check RPC
// so any service that exports the health protocol can be probed without
// custom integration.
package transport

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/SAY-5/edgemesh/internal/peer"
)

// GRPCProber probes peers via the standard gRPC health protocol.
//
// Connections are pooled per address with a per-call timeout. The prober is
// safe for concurrent use.
type GRPCProber struct {
	mu    sync.Mutex
	conns map[string]*grpc.ClientConn
	dial  func(ctx context.Context, addr string) (*grpc.ClientConn, error)
}

// NewGRPCProber constructs a prober. Pass nil dialer to use the default
// insecure dialer (only suitable for localhost / in-cluster traffic).
func NewGRPCProber(dial func(ctx context.Context, addr string) (*grpc.ClientConn, error)) *GRPCProber {
	if dial == nil {
		dial = defaultDial
	}
	return &GRPCProber{
		conns: make(map[string]*grpc.ClientConn),
		dial:  dial,
	}
}

func defaultDial(_ context.Context, addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
}

// Probe sends one Health/Check RPC and returns nil if the server reports
// SERVING.
func (p *GRPCProber) Probe(ctx context.Context, ep peer.Endpoint) error {
	conn, err := p.connect(ctx, ep.Address)
	if err != nil {
		return fmt.Errorf("prober dial %s: %w", ep.Address, err)
	}
	client := healthpb.NewHealthClient(conn)
	resp, err := client.Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		return err
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		return status.Errorf(codes.Unavailable, "peer %s reports %s", ep.ID, resp.GetStatus())
	}
	return nil
}

// Close drops every pooled connection.
func (p *GRPCProber) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	var first error
	for addr, c := range p.conns {
		if err := c.Close(); err != nil && first == nil {
			first = err
		}
		delete(p.conns, addr)
	}
	return first
}

func (p *GRPCProber) connect(ctx context.Context, addr string) (*grpc.ClientConn, error) {
	p.mu.Lock()
	c, ok := p.conns[addr]
	p.mu.Unlock()
	if ok {
		return c, nil
	}
	dctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	c, err := p.dial(dctx, addr)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	if existing, ok2 := p.conns[addr]; ok2 {
		// lost the race; close the new one and use the existing.
		_ = c.Close()
		c = existing
	} else {
		p.conns[addr] = c
	}
	p.mu.Unlock()
	return c, nil
}

// ErrNoConn is returned when the prober has no pooled connection for an
// address. Surfaced from utility helpers, not Probe itself.
var ErrNoConn = errors.New("transport: no pooled connection")
