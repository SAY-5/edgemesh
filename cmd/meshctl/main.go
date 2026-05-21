// Command meshctl is a small client for the sidecar's Mesh service.
//
// Subcommands:
//
//	meshctl status -addr 127.0.0.1:8080
//	meshctl drain  -addr 127.0.0.1:8080 -grace 30
//	meshctl health -addr 127.0.0.1:8080
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"text/tabwriter"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	meshpb "github.com/SAY-5/edgemesh/proto/edgemesh"
)

var version = "dev"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd, args := os.Args[1], os.Args[2:]
	switch cmd {
	case "status":
		mustRun(cmdStatus(args))
	case "drain":
		mustRun(cmdDrain(args))
	case "health":
		mustRun(cmdHealth(args))
	case "version":
		fmt.Println(version)
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: meshctl <status|drain|health|version> [flags]")
}

func mustRun(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func dial(addr string) (*grpc.ClientConn, error) {
	return grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
}

func cmdStatus(args []string) error {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:8080", "sidecar address")
	if err := fs.Parse(args); err != nil {
		return err
	}
	conn, err := dial(*addr)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := meshpb.NewMeshClient(conn).ListPeers(ctx, &meshpb.ListPeersRequest{})
	if err != nil {
		return err
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	if _, err := fmt.Fprintln(w, "PEER\tSERVICE\tADDRESS\tHEALTH\tSUCC\tFAIL\tINFLIGHT"); err != nil {
		return err
	}
	for _, p := range resp.GetPeers() {
		if _, err := fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%d\t%d\t%d\n",
			p.GetPeerId(), p.GetService(), p.GetAddress(),
			p.GetHealth(), p.GetConsecutiveSuccesses(), p.GetConsecutiveFailures(), p.GetInFlight()); err != nil {
			return err
		}
	}
	return w.Flush()
}

func cmdDrain(args []string) error {
	fs := flag.NewFlagSet("drain", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:8080", "sidecar address")
	grace := fs.Uint("grace", 30, "grace period seconds")
	if err := fs.Parse(args); err != nil {
		return err
	}
	conn, err := dial(*addr)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := meshpb.NewMeshClient(conn).Drain(ctx, &meshpb.DrainRequest{GraceSeconds: uint32(*grace)})
	if err != nil {
		return err
	}
	fmt.Println(resp.GetMessage())
	return nil
}

func cmdHealth(args []string) error {
	fs := flag.NewFlagSet("health", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:8080", "sidecar address")
	if err := fs.Parse(args); err != nil {
		return err
	}
	conn, err := dial(*addr)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	if err != nil {
		return err
	}
	fmt.Println(resp.GetStatus())
	return nil
}
