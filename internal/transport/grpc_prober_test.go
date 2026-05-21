package transport

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"

	"github.com/SAY-5/edgemesh/internal/peer"
)

type fakeHealth struct {
	healthpb.UnimplementedHealthServer
	status healthpb.HealthCheckResponse_ServingStatus
}

func (f *fakeHealth) Check(_ context.Context, _ *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	return &healthpb.HealthCheckResponse{Status: f.status}, nil
}

func proberWithFake(t *testing.T, status healthpb.HealthCheckResponse_ServingStatus) (*GRPCProber, func()) {
	t.Helper()
	lis := bufconn.Listen(1 << 16)
	srv := grpc.NewServer()
	healthpb.RegisterHealthServer(srv, &fakeHealth{status: status})
	go func() { _ = srv.Serve(lis) }()
	dial := func(_ context.Context, _ string) (*grpc.ClientConn, error) {
		return grpc.NewClient("passthrough://bufnet",
			grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) { return lis.Dial() }),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
	}
	prober := NewGRPCProber(dial)
	return prober, func() { srv.Stop(); _ = prober.Close() }
}

func TestProberSucceedsOnServing(t *testing.T) {
	p, stop := proberWithFake(t, healthpb.HealthCheckResponse_SERVING)
	defer stop()
	if err := p.Probe(context.Background(), peer.Endpoint{ID: "a", Address: "127.0.0.1:0"}); err != nil {
		t.Fatalf("expected success: %v", err)
	}
}

func TestProberFailsOnNotServing(t *testing.T) {
	p, stop := proberWithFake(t, healthpb.HealthCheckResponse_NOT_SERVING)
	defer stop()
	if err := p.Probe(context.Background(), peer.Endpoint{ID: "a", Address: "127.0.0.1:0"}); err == nil {
		t.Fatal("expected unavailable")
	}
}

func TestProberPoolsConnections(t *testing.T) {
	p, stop := proberWithFake(t, healthpb.HealthCheckResponse_SERVING)
	defer stop()
	ep := peer.Endpoint{ID: "a", Address: "addr-1"}
	for i := 0; i < 5; i++ {
		if err := p.Probe(context.Background(), ep); err != nil {
			t.Fatal(err)
		}
	}
	if len(p.conns) != 1 {
		t.Fatalf("expected exactly one pooled conn, got %d", len(p.conns))
	}
}
