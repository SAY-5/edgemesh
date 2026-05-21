// Package config loads the sidecar YAML configuration.
package config

import (
	"errors"
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level sidecar configuration.
type Config struct {
	NodeID   string        `yaml:"node_id"`
	Listen   Listen        `yaml:"listen"`
	Health   HealthConfig  `yaml:"health"`
	Retry    RetryConfig   `yaml:"retry"`
	LB       string        `yaml:"load_balancer"` // round-robin | least-pending
	Services []ServiceSpec `yaml:"services"`
}

// Listen describes what address the sidecar's gRPC server binds.
type Listen struct {
	Address    string `yaml:"address"`     // e.g. "127.0.0.1:8080"
	UnixSocket string `yaml:"unix_socket"` // optional, mutually exclusive with Address
}

type HealthConfig struct {
	IntervalMs         uint32 `yaml:"interval_ms"`
	TimeoutMs          uint32 `yaml:"timeout_ms"`
	HealthyToUnhealthy uint32 `yaml:"healthy_to_unhealthy"`
	UnhealthyToHealthy uint32 `yaml:"unhealthy_to_healthy"`
}

func (h HealthConfig) Interval() time.Duration {
	if h.IntervalMs == 0 {
		return time.Second
	}
	return time.Duration(h.IntervalMs) * time.Millisecond
}

func (h HealthConfig) Timeout() time.Duration {
	if h.TimeoutMs == 0 {
		return 200 * time.Millisecond
	}
	return time.Duration(h.TimeoutMs) * time.Millisecond
}

type RetryConfig struct {
	MaxAttempts uint32  `yaml:"max_attempts"`
	BaseMs      uint32  `yaml:"base_ms"`
	Multiplier  float64 `yaml:"multiplier"`
	MaxMs       uint32  `yaml:"max_ms"`
	JitterFrac  float64 `yaml:"jitter_frac"`
}

type ServiceSpec struct {
	Name    string   `yaml:"name"`
	Peers   []Peer   `yaml:"peers"`
	Methods []Method `yaml:"methods"`
}

type Peer struct {
	ID      string `yaml:"id"`
	Address string `yaml:"address"`
}

type Method struct {
	Name       string `yaml:"name"`
	Idempotent bool   `yaml:"idempotent"`
}

// Load reads and validates a YAML configuration file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	var c Config
	if err := yaml.Unmarshal(data, &c); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return &c, nil
}

// Validate checks for obvious misconfiguration.
func (c *Config) Validate() error {
	if c.NodeID == "" {
		return errors.New("config: node_id required")
	}
	if c.Listen.Address == "" && c.Listen.UnixSocket == "" {
		return errors.New("config: listen.address or listen.unix_socket required")
	}
	if len(c.Services) == 0 {
		return errors.New("config: at least one service required")
	}
	for i, svc := range c.Services {
		if svc.Name == "" {
			return fmt.Errorf("config: services[%d].name required", i)
		}
		if len(svc.Peers) == 0 {
			return fmt.Errorf("config: services[%d].peers required", i)
		}
		for j, p := range svc.Peers {
			if p.ID == "" {
				return fmt.Errorf("config: services[%d].peers[%d].id required", i, j)
			}
			if p.Address == "" {
				return fmt.Errorf("config: services[%d].peers[%d].address required", i, j)
			}
		}
	}
	if c.LB != "" && c.LB != "round-robin" && c.LB != "least-pending" {
		return fmt.Errorf("config: unknown load_balancer %q", c.LB)
	}
	return nil
}

// IsIdempotent reports whether a method on a service is configured as
// idempotent and therefore eligible for retry.
func (c *Config) IsIdempotent(service, method string) bool {
	for _, svc := range c.Services {
		if svc.Name != service {
			continue
		}
		for _, m := range svc.Methods {
			if m.Name == method {
				return m.Idempotent
			}
		}
	}
	return false
}
