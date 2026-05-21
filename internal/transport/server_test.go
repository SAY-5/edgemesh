package transport

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/test/bufconn"

	"github.com/SAY-5/edgemesh/internal/config"
	"github.com/SAY-5/edgemesh/internal/lb"
	"github.com/SAY-5/edgemesh/internal/peer"
	meshpb "github.com/SAY-5/edgemesh/proto/edgemesh"
)

const bufSize = 1 << 16

func newTestServer(t *testing.T) (*grpc.Server, *bufconn.Listener, *Sidecar) {
	t.Helper()
	cfg := &config.Config{
		NodeID: "node-a",
		Listen: config.Listen{Address: "127.0.0.1:0"},
		LB:     "round-robin",
		Services: []config.ServiceSpec{
			{Name: "echo", Peers: []config.Peer{
				{ID: "p1", Address: "127.0.0.1:9001"},
				{ID: "p2", Address: "127.0.0.1:9002"},
			}},
		},
	}
	hc := peer.NewHealthChecker(peer.ProberFunc(func(_ context.Context, _ peer.Endpoint) error { return nil }), 0, 0)
	for _, p := range cfg.Services[0].Peers {
		tr := peer.NewTracker(peer.Endpoint{ID: p.ID, Service: "echo", Address: p.Address}, peer.DefaultThresholds())
		// pre-mark healthy so Forward succeeds
		tr.RecordSuccess(time.Now())
		hc.Register(tr)
	}
	s := New(cfg, hc, lb.FromName(cfg.LB))
	srv := grpc.NewServer()
	s.Register(srv)
	lis := bufconn.Listen(bufSize)
	go func() { _ = srv.Serve(lis) }()
	return srv, lis, s
}

func dialBuf(lis *bufconn.Listener) (*grpc.ClientConn, error) {
	return grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(_ context.Context, _ string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
}

func TestServerListPeers(t *testing.T) {
	srv, lis, _ := newTestServer(t)
	defer srv.Stop()
	conn, err := dialBuf(lis)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := meshpb.NewMeshClient(conn)
	resp, err := client.ListPeers(context.Background(), &meshpb.ListPeersRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetPeers()) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(resp.GetPeers()))
	}
}

func TestServerGetPeerNotFound(t *testing.T) {
	srv, lis, _ := newTestServer(t)
	defer srv.Stop()
	conn, err := dialBuf(lis)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := meshpb.NewMeshClient(conn)
	if _, err := client.GetPeer(context.Background(), &meshpb.GetPeerRequest{PeerId: "missing"}); err == nil {
		t.Fatal("expected NotFound")
	}
}

func TestServerDrainTogglesHealth(t *testing.T) {
	srv, lis, _ := newTestServer(t)
	defer srv.Stop()
	conn, err := dialBuf(lis)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	mclient := meshpb.NewMeshClient(conn)
	hclient := healthpb.NewHealthClient(conn)

	resp, err := hclient.Check(context.Background(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Fatalf("expected SERVING, got %s", resp.GetStatus())
	}
	if _, err := mclient.Drain(context.Background(), &meshpb.DrainRequest{}); err != nil {
		t.Fatal(err)
	}
	resp, err = hclient.Check(context.Background(), &healthpb.HealthCheckRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_NOT_SERVING {
		t.Fatalf("expected NOT_SERVING after drain, got %s", resp.GetStatus())
	}
}

func TestServerForwardPicksHealthyPeer(t *testing.T) {
	srv, lis, _ := newTestServer(t)
	defer srv.Stop()
	conn, err := dialBuf(lis)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := meshpb.NewMeshClient(conn)
	resp, err := client.Forward(context.Background(), &meshpb.ForwardRequest{Service: "echo", Method: "Say", Payload: []byte("hello")})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetPeerId() == "" {
		t.Fatal("no peer chosen")
	}
}
