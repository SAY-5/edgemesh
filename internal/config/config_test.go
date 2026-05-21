package config

import (
	"os"
	"path/filepath"
	"testing"
)

const goodYAML = `
node_id: node-a
listen:
  address: 127.0.0.1:8080
load_balancer: round-robin
health:
  interval_ms: 500
  timeout_ms: 100
  healthy_to_unhealthy: 3
  unhealthy_to_healthy: 2
retry:
  max_attempts: 3
  base_ms: 50
  multiplier: 4
  max_ms: 1000
  jitter_frac: 0.2
services:
  - name: echo
    peers:
      - id: node-b
        address: 127.0.0.1:8081
      - id: node-c
        address: 127.0.0.1:8082
    methods:
      - name: Say
        idempotent: true
`

func writeTemp(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	p := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestLoadValid(t *testing.T) {
	p := writeTemp(t, goodYAML)
	c, err := Load(p)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if c.NodeID != "node-a" {
		t.Fatalf("node id: %q", c.NodeID)
	}
	if len(c.Services) != 1 || len(c.Services[0].Peers) != 2 {
		t.Fatalf("unexpected service shape: %+v", c.Services)
	}
	if !c.IsIdempotent("echo", "Say") {
		t.Fatal("expected idempotent")
	}
	if c.IsIdempotent("echo", "NotAMethod") {
		t.Fatal("unknown method should not be idempotent")
	}
	if c.Health.Interval().Milliseconds() != 500 {
		t.Fatalf("interval wrong: %v", c.Health.Interval())
	}
}

func TestLoadMissingFile(t *testing.T) {
	if _, err := Load("/no/such/file.yaml"); err == nil {
		t.Fatal("expected error")
	}
}

func TestValidateErrors(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"no node_id", "listen:\n  address: 127.0.0.1:1\nservices:\n  - name: x\n    peers:\n      - id: a\n        address: a\n"},
		{"no listen", "node_id: a\nservices:\n  - name: x\n    peers:\n      - id: a\n        address: a\n"},
		{"no services", "node_id: a\nlisten:\n  address: 127.0.0.1:1\n"},
		{"missing peer addr", "node_id: a\nlisten:\n  address: 127.0.0.1:1\nservices:\n  - name: x\n    peers:\n      - id: a\n"},
		{"unknown lb", "node_id: a\nlisten:\n  address: 127.0.0.1:1\nload_balancer: bogus\nservices:\n  - name: x\n    peers:\n      - id: a\n        address: a\n"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := writeTemp(t, c.body)
			if _, err := Load(p); err == nil {
				t.Fatalf("expected validation error")
			}
		})
	}
}
