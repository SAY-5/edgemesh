// Command sidecar runs an EdgeMesh sidecar.
//
// The binary loads a YAML config, registers every peer with the health
// checker, starts the periodic prober, and listens on the configured
// address. Shutdown drains the gRPC server cleanly on SIGINT / SIGTERM.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/SAY-5/edgemesh/internal/config"
	"github.com/SAY-5/edgemesh/internal/lb"
	"github.com/SAY-5/edgemesh/internal/peer"
	"github.com/SAY-5/edgemesh/internal/transport"
)

var version = "dev"

func main() {
	if err := run(); err != nil {
		log.Fatalf("sidecar: %v", err)
	}
}

func run() error {
	cfgPath := flag.String("config", envOr("EDGEMESH_CONFIG", "config.yaml"), "path to YAML config")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Println(version)
		return nil
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}

	prober := transport.NewGRPCProber(nil)
	hc := peer.NewHealthChecker(prober, cfg.Health.Interval(), cfg.Health.Timeout())
	thresh := peer.Thresholds{
		HealthyToUnhealthy: cfg.Health.HealthyToUnhealthy,
		UnhealthyToHealthy: cfg.Health.UnhealthyToHealthy,
	}
	for _, svc := range cfg.Services {
		for _, p := range svc.Peers {
			hc.Register(peer.NewTracker(peer.Endpoint{ID: p.ID, Service: svc.Name, Address: p.Address}, thresh))
		}
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go hc.Run(ctx)

	listener, err := transport.Listen(cfg.Listen)
	if err != nil {
		return err
	}
	srv := grpc.NewServer()
	sidecar := transport.New(cfg, hc, lb.FromName(cfg.LB))
	sidecar.Register(srv)

	log.Printf("sidecar %s listening on %s (lb=%s, peers=%d)", cfg.NodeID, listener.Addr(), cfg.LB, totalPeers(cfg))

	go func() {
		<-ctx.Done()
		// give in-flight RPCs a brief window before forcing close
		stopped := make(chan struct{})
		go func() { srv.GracefulStop(); close(stopped) }()
		select {
		case <-stopped:
		case <-time.After(5 * time.Second):
			srv.Stop()
		}
		_ = prober.Close()
	}()

	if err := srv.Serve(listener); err != nil && ctx.Err() == nil {
		return fmt.Errorf("grpc serve: %w", err)
	}
	return nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func totalPeers(c *config.Config) int {
	n := 0
	for _, s := range c.Services {
		n += len(s.Peers)
	}
	return n
}
