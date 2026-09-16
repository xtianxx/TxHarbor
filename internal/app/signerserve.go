package app

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"

	"github.com/xtianxx/txharbor/internal/config"
	"github.com/xtianxx/txharbor/internal/db"
	"github.com/xtianxx/txharbor/internal/logx"
	"github.com/xtianxx/txharbor/internal/metrics"
	"github.com/xtianxx/txharbor/internal/signer"
)

// SignerServe runs `txharbor signer-serve`: the standalone signer process,
// the only binary path that constructs a KeyProvider. Order: config gate
// (full Load validation + signer required-ness) → pool → metrics → policy →
// provider → listener → serve until ctx ends. Exit 0 on clean shutdown after
// SIGINT/SIGTERM, 1 on startup failure (redacted reason).
//
// The submit/status route handlers land with US1/US6 (T016/T029); until then
// the mux answers the signer paths with 501 plus the shared metrics
// endpoint, so the listener, shutdown and config gate are independently
// verifiable now.
func SignerServe(ctx context.Context, args []string, d Deps) int {
	stderr := d.stderr()
	if len(args) != 0 {
		fmt.Fprintf(stderr, "txharbor signer-serve: takes no arguments\n")
		return 2
	}
	cfg, err := config.Load(d.getenv())
	if err != nil {
		fmt.Fprintf(stderr, "txharbor signer-serve: configuration error: %s\n", logx.Redact(err.Error()))
		return 1
	}
	policyCfg, err := cfg.SignerPolicyConfig()
	if err != nil {
		fmt.Fprintf(stderr, "txharbor signer-serve: configuration error: %s\n", logx.Redact(err.Error()))
		return 1
	}
	policy, err := signer.NewPolicy(policyCfg)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor signer-serve: configuration error: %s\n", logx.Redact(err.Error()))
		return 1
	}
	pool, err := db.OpenPool(ctx, cfg.PGDSN, cfg.ProbeTimeout)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor signer-serve: %s\n", logx.Redact(err.Error()))
		return 1
	}
	defer pool.Close()
	m := metrics.New(func() bool { return true })
	provider, err := signer.NewDevKeyProvider(signer.Mode(cfg.SignerMode), cfg.SignerKeyFile, cfg.SignerKeyTimeout)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor signer-serve: configuration error: %s\n", logx.Redact(err.Error()))
		return 1
	}
	_ = provider
	_ = policy

	mux := http.NewServeMux()
	mux.Handle("/metrics", m.Handler())
	mux.HandleFunc("/signer/v1/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotImplemented)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"code": "not_implemented",
			"msg":  "signer routes land with US1/US6",
		})
	})
	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: cfg.ProbeTimeout,
		WriteTimeout:      cfg.ProbeTimeout,
	}
	listener, err := net.Listen("tcp", cfg.SignerHTTPAddr)
	if err != nil {
		fmt.Fprintf(stderr, "txharbor signer-serve: startup failed (http listen): %s\n", logx.Redact(err.Error()))
		return 1
	}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()
	if err := srv.Serve(listener); err != nil && err != http.ErrServerClosed {
		fmt.Fprintf(stderr, "txharbor signer-serve: %s\n", logx.Redact(err.Error()))
		return 1
	}
	return 0
}
