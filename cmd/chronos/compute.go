package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"

	"github.com/felixgeelhaar/chronos"
	"github.com/felixgeelhaar/chronos/embed"
	"github.com/felixgeelhaar/chronos/internal/config"
	"github.com/felixgeelhaar/chronos/internal/notify"
	"github.com/felixgeelhaar/chronos/internal/observability"
	"github.com/felixgeelhaar/chronos/internal/ports"
	"github.com/google/uuid"
)

func runCompute(args []string) error {
	fs := flag.NewFlagSet("compute", flag.ContinueOnError)
	adapterName := fs.String("adapter", "", "adapter name (registered via chronos.Register from a blank-imported package)")
	scopeIDStr := fs.String("scope-id", "", "scope ID (UUID); alias for --coach-id retained for backward compatibility")
	coachIDStr := fs.String("coach-id", "", "deprecated alias for --scope-id; supplied to the adapter as cfg[\"coach_id\"] for back-compat with adapters that key on it")
	if err := fs.Parse(args); err != nil {
		return NewUserError("compute: %v", err)
	}

	if *adapterName == "" {
		return NewUserError("compute: --adapter is required")
	}
	scope := *scopeIDStr
	if scope == "" {
		scope = *coachIDStr
	}
	if scope == "" {
		return NewUserError("compute: --scope-id (or --coach-id) is required")
	}
	if _, err := uuid.Parse(scope); err != nil {
		return NewUserError("compute: invalid scope ID %q: %v", scope, err)
	}

	cfg := config.Default()
	if err := cfg.Validate(); err != nil {
		return NewUserError("compute: invalid configuration: %v", err)
	}

	src, ok := chronos.Get(*adapterName)
	if !ok {
		return NewNotFoundError("adapter %q not registered (available: %v)", *adapterName, chronos.Adapters())
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.ComputationTimeout)
	defer cancel()

	dsn, err := resolveDSN(cfg)
	if err != nil {
		return NewUserError("compute: %v", err)
	}

	logger := slog.Default().With("cmd", "compute", "adapter", *adapterName)
	metrics := observability.New()

	opts := []embed.Option{
		embed.WithStorage(dsn),
		embed.WithDetectionConfig(cfg),
		embed.WithLogger(logger),
		embed.WithAdapterName(src.Name()),
		embed.WithDetectorMetrics(metrics),
	}
	if cfg.DetectorParallelism {
		opts = append(opts, embed.WithParallelDetectors())
	}
	eng, err := embed.New(opts...)
	if err != nil {
		return NewSystemError(err, "compute: open embed engine: %v", err)
	}
	defer func() { _ = eng.Close() }()

	// Wrap signal persistence with configured push transports so newly
	// detected signals fan out to webhooks the moment they are saved.
	if n := buildNotifier(cfg, metrics, logger); n != nil {
		eng.SetSignalRepository(notify.WrapSignals(eng.SignalRepository(), n))
	}

	logger.Info("fetch begin", "adapter", src.Name())
	states, err := src.Fetch(ctx, map[string]string{"coach_id": scope, "scope_id": scope})
	if err != nil {
		return NewSystemError(err, "compute: fetch: %v", err)
	}
	logger.Info("fetch complete", "adapter", src.Name(), "states", len(states))
	if len(states) == 0 {
		fmt.Printf("Fetched 0 entity states; emitted 0 signals.\n")
		return nil
	}

	if err := eng.ProcessBatch(ctx, states); err != nil {
		return NewSystemError(err, "compute: persist states: %v", err)
	}
	metrics.ObserveObservations(src.Name(), len(states))

	signals, err := eng.DetectStates(ctx, states)
	if err != nil {
		return NewSystemError(err, "compute: detect: %v", err)
	}
	for _, sig := range signals {
		metrics.ObserveSignal(string(sig.Pattern))
	}

	fmt.Printf("Fetched %d entity states; emitted %d signals.\n", len(states), len(signals))
	return nil
}

// buildNotifier assembles a Multi notifier from the configured push
// transports. Returns nil when nothing is configured so the call site
// can pass it to wrapWithNotifier unconditionally.
func buildNotifier(cfg *config.Config, metrics *observability.Metrics, logger *slog.Logger) ports.Notifier {
	var ns notify.Multi
	if len(cfg.WebhookURLs) > 0 {
		wh := notify.NewWebhook(notify.WebhookConfig{
			URLs:    cfg.WebhookURLs,
			Secret:  cfg.WebhookSecret,
			Timeout: cfg.WebhookTimeout,
			Retries: cfg.WebhookRetries,
		}, metrics, logger)
		ns = append(ns, wh)
	}
	if len(ns) == 0 {
		return nil
	}
	return ns
}

// wrapWithNotifier returns the bare repository when notifier is nil so
// we don't pay a wrapper cost for the common no-push case.
func wrapWithNotifier(repo ports.SignalRepository, notifier ports.Notifier) ports.SignalRepository {
	if notifier == nil {
		return repo
	}
	return notify.WrapSignals(repo, notifier)
}
