// Package testutil provides Docker-backed helpers for the 013 integration
// layers (build tags integration_redis / integration_kafka). It is test
// support only: production packages never import it, and `make test`
// (untagged) never starts a container.
//
// Both helpers bind their container port to a fixed loopback host port
// (chosen free at start) instead of testcontainers' ephemeral allocation.
// That makes the address stable across Stop/Start fault injection: a client
// built before an outage reconnects to the same address after recovery, and
// the Kafka module's advertised listener (baked into its startup script)
// keeps matching the Docker port mapping. Both helpers pin the container
// images that compose.yaml pins for the local drill, so tests and the local
// environment speak the same broker versions.
package testutil

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// RedisImage is the pinned Redis image (T004 compose pins the same tag).
// Verified available on 2026-09-24 (T002/T004 evidence).
const RedisImage = "redis:8.2.10-alpine"

// Redis wraps a testcontainers Redis instance with stop/start fault-injection
// hooks. The container port is bound to a fixed loopback host port, so Addr
// stays valid across a restart.
type Redis struct {
	container *tcredis.RedisContainer
	addr      string
}

// StartRedis boots one Redis container and waits until it accepts
// connections. Callers own the lifecycle and MUST call Close.
func StartRedis(ctx context.Context) (*Redis, error) {
	port, err := freeLoopbackPort()
	if err != nil {
		return nil, err
	}
	ctr, err := tcredis.Run(ctx, RedisImage,
		// Randomized container name so parallel runs never collide (resource
		// naming discipline; testcontainers would also pick a random name,
		// but an explicit prefix keeps `docker ps` readable).
		withTestName("redis"),
		withFixedLoopbackPort("6379/tcp", port),
	)
	if err != nil {
		return nil, fmt.Errorf("start redis container: %w", err)
	}
	addr, err := ctr.ConnectionString(ctx)
	if err != nil {
		_ = ctr.Terminate(context.Background())
		return nil, fmt.Errorf("redis connection string: %w", err)
	}
	return &Redis{container: ctr, addr: addr}, nil
}

// Addr returns the redis:// connection string.
func (r *Redis) Addr() string { return r.addr }

// Stop stops the container: fault injection for "Redis unavailable".
func (r *Redis) Stop(ctx context.Context) error {
	if err := r.container.Stop(ctx, nil); err != nil {
		return fmt.Errorf("stop redis container: %w", err)
	}
	return nil
}

// Start restarts a stopped container (recovery) and waits for readiness. The
// address is stable (fixed host port), so pre-outage clients reconnect.
func (r *Redis) Start(ctx context.Context) error {
	if err := r.container.Start(ctx); err != nil {
		return fmt.Errorf("start redis container: %w", err)
	}
	return nil
}

// Close terminates the container.
func (r *Redis) Close(ctx context.Context) error {
	if err := r.container.Terminate(ctx); err != nil {
		return fmt.Errorf("terminate redis container: %w", err)
	}
	return nil
}

// freeLoopbackPort reserves a free 127.0.0.1 port and releases it for the
// container binding. A concurrent grab between probe and start is possible
// but the run fails loudly rather than silently.
func freeLoopbackPort() (int, error) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("reserve loopback port: %w", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

// withFixedLoopbackPort binds one container port to a fixed 127.0.0.1 host
// port (testcontainers' default ephemeral allocation would change the address
// on every restart).
func withFixedLoopbackPort(containerPort string, hostPort int) testcontainers.CustomizeRequestOption {
	return testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
		if hc.PortBindings == nil {
			hc.PortBindings = network.PortMap{}
		}
		hc.PortBindings[network.MustParsePort(containerPort)] = []network.PortBinding{
			{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: strconv.Itoa(hostPort)},
		}
	})
}

// randomSuffix returns 8 hex characters for container/topic name
// randomization.
func randomSuffix() string {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano()&0xffffffff)
	}
	return hex.EncodeToString(b[:])
}
