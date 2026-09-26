package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	chronosv1 "github.com/felixgeelhaar/chronos/api/proto/chronos/v1"
	"github.com/felixgeelhaar/chronos/internal/api"
	grpctransport "github.com/felixgeelhaar/chronos/internal/api/grpc"
	"github.com/felixgeelhaar/chronos/internal/config"
	"github.com/felixgeelhaar/chronos/internal/notify"
	"github.com/felixgeelhaar/chronos/internal/observability"
	"github.com/felixgeelhaar/chronos/internal/pipeline"
	"github.com/felixgeelhaar/chronos/internal/ports"
	"github.com/felixgeelhaar/chronos/internal/store"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcmetadata "google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func runServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ContinueOnError)
	port := fs.Int("port", 0, "HTTP port (overrides CHRONOS_HTTP_PORT)")
	host := fs.String("host", "", "HTTP host (overrides CHRONOS_HTTP_HOST)")
	grpcPort := fs.Int("grpc-port", 0, "gRPC port (overrides CHRONOS_GRPC_PORT)")
	if err := fs.Parse(args); err != nil {
		return NewUserError("serve: %v", err)
	}

	cfg := config.Default()
	if *port > 0 {
		cfg.HTTPPort = *port
	}
	if *host != "" {
		cfg.HTTPHost = *host
	}
	if *grpcPort > 0 {
		cfg.GRPCPort = *grpcPort
	}
	if err := cfg.Validate(); err != nil {
		return NewUserError("serve: invalid configuration: %v", err)
	}

	// Constructed before the store opens so the connect retry has
	// somewhere to report; nothing above this point logs.
	logger := slog.Default().With("cmd", "serve")

	dsn, err := resolveDSN(cfg)
	if err != nil {
		return NewUserError("serve: %v", err)
	}
	conn, err := openStoreWithRetry(context.Background(), dsn, logger)
	if err != nil {
		return NewSystemError(err, "serve: open store: %v", err)
	}
	defer func() { _ = conn.Close() }()

	metrics := observability.New()

	// Build the notifier set: webhooks (cross-process) + SSE
	// (in-process / browser). Both implement ports.Notifier and fan
	// out off the same Save call. SSE lives in this process; signals
	// reach it only when the in-process detection scheduler runs, so
	// we pair them: scheduler enabled <=> SSE useful.
	var notifiers notify.Multi
	if len(cfg.WebhookURLs) > 0 {
		notifiers = append(notifiers, notify.NewWebhook(notify.WebhookConfig{
			URLs:    cfg.WebhookURLs,
			Secret:  cfg.WebhookSecret,
			Timeout: cfg.WebhookTimeout,
			Retries: cfg.WebhookRetries,
		}, metrics, logger))
	}
	var sse *notify.SSE
	if cfg.DetectionInterval > 0 {
		sse = notify.NewSSE(16) // small per-client buffer; slow clients are dropped
		notifiers = append(notifiers, sse)
	}
	var notifier ports.Notifier
	if len(notifiers) > 0 {
		notifier = notifiers
	}

	signals := wrapWithNotifier(conn.Signals, notifier)

	srv := api.NewServer(conn.EntityStates, signals, metrics, logger)
	if sse != nil {
		srv = srv.WithSSE(sse)
	}
	mux := http.NewServeMux()
	srv.RegisterRoutes(mux)

	// Recover is the outermost layer so panics never escape the HTTP
	// server; Logging records every request once it has been served;
	// BearerAuth gates the API when CHRONOS_API_TOKEN is set (no-op
	// when empty so the default development experience is unchanged).
	handler := api.Chain(mux,
		api.Recover(logger),
		api.Logging(logger, metrics),
		api.BearerAuth(cfg.APIToken),
	)
	if cfg.APIToken != "" {
		logger.Info("api auth enabled", "scheme", "bearer")
	}

	addr := fmt.Sprintf("%s:%d", cfg.HTTPHost, cfg.HTTPPort)
	httpSrv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// gRPC server — optional, started only when GRPCPort > 0.
	var grpcSrv *grpc.Server
	var grpcListener net.Listener
	if cfg.GRPCPort > 0 {
		grpcAddr := fmt.Sprintf("%s:%d", cfg.GRPCHost, cfg.GRPCPort)
		var err error
		grpcListener, err = net.Listen("tcp", grpcAddr)
		if err != nil {
			return NewSystemError(err, "serve: grpc listen: %v", err)
		}
		grpcOpts := []grpc.ServerOption{}
		if cfg.APIToken != "" {
			grpcOpts = append(grpcOpts,
				grpc.UnaryInterceptor(bearerAuthInterceptor(cfg.APIToken)),
				grpc.StreamInterceptor(bearerAuthStreamInterceptor(cfg.APIToken)),
			)
		}
		grpcSrv = grpc.NewServer(grpcOpts...)
		grpcImpl := grpctransport.NewServer(conn.EntityStates, signals, metrics, logger)
		if sse != nil {
			grpcImpl = grpcImpl.WithStreamer(sse)
		}
		chronosv1.RegisterChronosServiceServer(grpcSrv, grpcImpl)
		go func() {
			logger.Info("grpc listening", "addr", grpcAddr)
			if err := grpcSrv.Serve(grpcListener); err != nil {
				logger.Error("grpc server exited", "err", err)
			}
		}()
	}

	// rootCtx propagates shutdown to background goroutines (scheduler,
	// SSE drain). The HTTP server has its own Shutdown; cancelling
	// rootCtx unwinds everything else.
	rootCtx, cancelRoot := context.WithCancel(context.Background())
	defer cancelRoot()

	// In-process detection scheduler. Disabled when interval == 0 so
	// `serve` keeps its historical behaviour: streaming ingest only,
	// no detection. Enabled, it ticks Engine.Detect over each scope's
	// observations and writes signals via the notifier-wrapped repo —
	// which is what makes SSE see anything.
	if cfg.DetectionInterval > 0 {
		sched := pipeline.NewScheduler(conn.EntityStates, signals, pipeline.NewEngine(cfg).WithMetrics(metrics), cfg.DetectionInterval, logger).
			WithRetention(cfg.SignalRetention).
			WithLookback(cfg.DetectionLookback).
			WithSweepInterval(cfg.RetentionSweepInterval).
			WithMetrics(metrics)
		go func() {
			if err := sched.Run(rootCtx); err != nil {
				logger.Error("scheduler exited with error", "err", err)
			}
		}()
	}

	// Graceful shutdown on SIGINT / SIGTERM. Stops accepting new
	// requests; lets in-flight ones drain for up to 10 seconds.
	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		logger.Info("shutdown signal received")
		cancelRoot() // stop the scheduler before draining HTTP
		if grpcSrv != nil {
			grpcSrv.GracefulStop()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(ctx)
	}()

	logger.Info("listening", "addr", addr, "store", storeKind(dsn),
		"detection_interval", cfg.DetectionInterval, "detection_lookback", cfg.DetectionLookback,
		"signal_retention", cfg.SignalRetention,
		"retention_sweep_interval", cfg.RetentionSweepInterval,
		"webhooks", len(cfg.WebhookURLs),
		"grpc_port", cfg.GRPCPort)
	if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return NewSystemError(err, "serve: %v", err)
	}
	return nil
}

// bearerAuthInterceptor returns a gRPC unary interceptor that rejects
// requests whose "authorization" metadata does not contain "Bearer <token>".
// When token is empty the interceptor is a no-op.
func bearerAuthInterceptor(token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := checkBearer(ctx, token); err != nil {
			return nil, err
		}
		return handler(ctx, req)
	}
}

func bearerAuthStreamInterceptor(token string) grpc.StreamServerInterceptor {
	return func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := checkBearer(ss.Context(), token); err != nil {
			return err
		}
		return handler(srv, ss)
	}
}

func checkBearer(ctx context.Context, token string) error {
	md, ok := grpcmetadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "missing metadata")
	}
	auth := md.Get("authorization")
	if len(auth) == 0 || auth[0] != "Bearer "+token {
		return status.Error(codes.Unauthenticated, "invalid token")
	}
	return nil
}

// storeConnectAttempts and storeConnectBackoff bound the wait for a
// database that is reachable a moment later than the process starts.
const (
	storeConnectAttempts = 5
	storeConnectBackoff  = 2 * time.Second
)

// openStoreWithRetry opens the store, retrying a failing connection a
// bounded number of times before giving up.
//
// Starting under an orchestrator regularly loses a race against the
// process's own networking: the container is running before the
// service routing into its namespace is programmed, so the very first
// dial is refused and every subsequent one succeeds. Observed on every
// single rollout of this engine, with identical start and finish
// timestamps and a database that had not moved in nine hours.
//
// Without this the process exits, which an orchestrator papers over by
// restarting it -- but it burns a restart on every deploy, and the
// restart counter is exactly the instrument an operator watches to
// decide whether a memory fix worked. A misconfigured DSN still fails,
// because it fails identically on every attempt.
func openStoreWithRetry(ctx context.Context, dsn string, logger *slog.Logger) (*store.Conn, error) {
	var err error
	for attempt := 1; attempt <= storeConnectAttempts; attempt++ {
		var conn *store.Conn
		conn, err = store.Open(ctx, dsn)
		if err == nil {
			if attempt > 1 && logger != nil {
				logger.Info("store connected after retry", "attempts", attempt)
			}
			return conn, nil
		}
		if attempt == storeConnectAttempts {
			break
		}
		if logger != nil {
			// The error carries host and database but not the password;
			// the DSN itself is never logged.
			logger.Warn("store connect failed; retrying",
				"attempt", attempt, "of", storeConnectAttempts,
				"backoff", storeConnectBackoff, "err", err)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(storeConnectBackoff):
		}
	}
	return nil, err
}

// storeKind names the backend for logging, derived from the DSN that
// actually selected it.
//
// It used to log cfg.DBType, which is the legacy selector and defaults
// to "sqlite" whether or not it is in use. resolveDSN prefers
// CHRONOS_DB_DSN and only falls back to DBType, so any deployment
// configured the modern way -- the only way to pass a ?namespace=
// parameter -- announced "store=sqlite" while running on Postgres. A
// line that names the wrong storage backend is worse than no line,
// because it gets believed.
//
// Only the scheme is returned. The DSN carries credentials and must
// never reach a log.
func storeKind(dsn string) string {
	if i := strings.Index(dsn, "://"); i > 0 {
		return dsn[:i]
	}
	return "unknown"
}
