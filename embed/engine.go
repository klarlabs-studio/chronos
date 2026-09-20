// Package embed exposes Chronos as an in-process Go library. Use this
// package when you want to embed Chronos directly into your application
// (an agent runtime, a memory layer, a test harness) instead of running
// it as a CLI or HTTP service.
//
// A typical wiring:
//
//	eng, err := embed.New(embed.WithStorage("sqlite:///app.db?namespace=mychronos"))
//	if err != nil { return err }
//	defer eng.Close()
//
//	state := chronos.EntityState{
//	    ID:        uuid.New(),
//	    EntityID:  someEntityID,
//	    ScopeID:   someScopeID,
//	    Timestamp: time.Now(),
//	    Features:  []float64{42, 0.85, 1.0},
//	}
//	if err := eng.Process(ctx, state); err != nil { return err }
//
//	signals, err := eng.Detect(ctx, []uuid.UUID{someScopeID})
//	// or:
//	signals, err = eng.Query(ctx, embed.QueryOpts{ScopeID: someScopeID})
//
// The CLI (`cmd/chronos compute`) dogfoods this package: it constructs
// an Engine via New, ProcessBatch-es adapter output, then DetectStates.
// The HTTP `serve` path still wires internal/pipeline directly.
package embed

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/felixgeelhaar/chronos/internal/config"
	"github.com/felixgeelhaar/chronos/internal/detect"
	"github.com/felixgeelhaar/chronos/internal/observability"
	"github.com/felixgeelhaar/chronos/internal/ports"
	"github.com/felixgeelhaar/chronos/internal/store"
	"github.com/google/uuid"

	// Register the in-memory provider by default so a zero-config
	// embed.New() works without the caller having to blank-import
	// anything. Durable SQL backends are opt-in: blank-import the public
	// shim for the one you want (avoids pulling pgx / mysql drivers into
	// memory-only consumers), e.g.
	//   _ "github.com/felixgeelhaar/chronos/storage/postgres"
	// or storage/all for every backend. (The providers themselves live
	// under internal/store and cannot be imported from outside the module,
	// which is why the public storage/* shims exist.)
	_ "github.com/felixgeelhaar/chronos/internal/store/memory"
)

// embeddedAdapterName is the default adapter origin for states
// pushed in via [Engine.Process] / [Engine.ProcessBatch]. Override with
// [WithAdapterName] when the host is a named Source (e.g. cmd/chronos compute).
const embeddedAdapterName = "embedded"

// Engine is the embeddable in-process Chronos engine. Constructed with
// [New]; call [Engine.Close] when finished to release the storage
// handle.
//
// Engine methods are safe for concurrent read use ([Engine.Query],
// [Engine.Detect] without state writes). Writes ([Engine.Process],
// [Engine.ProcessBatch]) should be serialised per scope to avoid the
// detector producing duplicate signals from races.
type Engine struct {
	conn        *store.Conn
	detector    *detect.Engine
	cfg         *config.Config
	logger      *slog.Logger
	adapterName string
	signals     ports.SignalRepository
}

// QueryOpts filters a [Engine.Query] result. At least one of ScopeID or
// ScopeIDs should be set for performance; unscoped queries scan the
// whole signal table.
type QueryOpts struct {
	ScopeID       uuid.UUID
	ScopeIDs      []uuid.UUID
	Series        *uuid.UUID
	Pattern       *chronos.PatternType
	Since         *time.Time
	Until         *time.Time
	MinConfidence *float64
	Limit         int
}

// engineConfig is the internal aggregate of all Option choices. Built
// by [New] from the supplied options.
type engineConfig struct {
	storageDSN    string
	storeRegistry *store.Registry
	logger        *slog.Logger
	cfg           *config.Config
	detectors     []detect.Detector
	parallel      bool
	adapterName   string
	metrics       *observability.Metrics
}

// Option configures [New]. Construct via the With* helpers.
type Option interface {
	applyOption(*engineConfig)
}

type optionFunc func(*engineConfig)

func (f optionFunc) applyOption(c *engineConfig) { f(c) }

// WithStorage overrides the storage DSN. Schemes registered in
// internal/store (memory, sqlite, postgres, mysql, libsql) are
// supported when their provider package is blank-imported by the
// consuming program. The default is "memory://?namespace=chronos".
func WithStorage(dsn string) Option {
	return optionFunc(func(c *engineConfig) { c.storageDSN = dsn })
}

// WithStoreRegistry overrides the provider registry used to resolve
// the storage DSN. Defaults to [store.DefaultRegistry]. In-module
// hosts and tests that need an isolated provider set pass
// store.DefaultRegistry().Clone() (or a [store.NewRegistry]) so
// Register calls do not affect other engines in the same process.
//
// External embedders normally leave this unset and blank-import the
// public storage/* shims, which register on the default registry.
func WithStoreRegistry(r *store.Registry) Option {
	return optionFunc(func(c *engineConfig) { c.storeRegistry = r })
}

// WithLogger overrides the [slog.Logger] the engine uses for internal
// diagnostics. Defaults to a discard logger so the embeddable surface
// is silent unless the host opts in.
func WithLogger(l *slog.Logger) Option {
	return optionFunc(func(c *engineConfig) { c.logger = l })
}

// WithDetectionConfig overrides the detector configuration. When unset,
// [config.Default] is used. Callers typically tweak individual
// thresholds rather than replacing the whole config.
func WithDetectionConfig(cfg *config.Config) Option {
	return optionFunc(func(c *engineConfig) { c.cfg = cfg })
}

// WithDetectors overrides the detector set. Pass an empty slice to
// disable detection entirely (the engine becomes a passive event log).
// When unset, [detect.DefaultDetectors] is used.
func WithDetectors(detectors ...detect.Detector) Option {
	return optionFunc(func(c *engineConfig) { c.detectors = detectors })
}

// WithParallelDetectors enables parallel detector execution. Detectors
// are pure functions of their inputs; enabling this is safe and trades
// CPU for wall-clock latency on multi-detector scopes.
func WithParallelDetectors() Option {
	return optionFunc(func(c *engineConfig) { c.parallel = true })
}

// WithAdapterName sets the adapter label recorded on ingested
// observations. Defaults to "embedded". cmd/chronos compute passes the
// Source name so retention and Count stay keyed correctly.
func WithAdapterName(name string) Option {
	return optionFunc(func(c *engineConfig) { c.adapterName = name })
}

// WithDetectorMetrics attaches a metrics registry so Detect records
// per-pattern latency / emit / skip counters (same as the HTTP server).
func WithDetectorMetrics(m *observability.Metrics) Option {
	return optionFunc(func(c *engineConfig) { c.metrics = m })
}

// New constructs an [Engine] from the supplied options. When no
// [WithStorage] is supplied the engine boots against an in-process
// memory store, which is suitable for tests and short-lived demos but
// not for production data retention.
//
// The returned Engine owns the storage handle and MUST be closed via
// [Engine.Close] when finished.
func New(opts ...Option) (*Engine, error) {
	cfg := engineConfig{
		storageDSN:  "memory://?namespace=chronos",
		logger:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		cfg:         config.Default(),
		parallel:    false,
		adapterName: embeddedAdapterName,
	}
	for _, opt := range opts {
		opt.applyOption(&cfg)
	}
	if cfg.adapterName == "" {
		cfg.adapterName = embeddedAdapterName
	}

	ctx := context.Background()
	reg := cfg.storeRegistry
	if reg == nil {
		reg = store.DefaultRegistry()
	}
	conn, err := reg.Open(ctx, cfg.storageDSN)
	if err != nil {
		return nil, fmt.Errorf("chronos/embed: open storage %q: %w", cfg.storageDSN, err)
	}

	detector := detect.NewEngine(cfg.cfg, cfg.detectors...)
	if cfg.parallel {
		detector = detector.WithParallelDetectors(true)
	}
	if cfg.metrics != nil {
		detector = detector.WithMetrics(cfg.metrics)
	}

	return &Engine{
		conn:        conn,
		detector:    detector,
		cfg:         cfg.cfg,
		logger:      cfg.logger,
		adapterName: cfg.adapterName,
		signals:     conn.Signals,
	}, nil
}

// SetSignalRepository replaces the repository Detect uses to persist
// signals. Intended for in-module hosts (cmd/chronos) that wrap the
// store with webhook notifiers. Must be called before Detect /
// DetectStates. External embedders normally leave the default.
func (e *Engine) SetSignalRepository(repo ports.SignalRepository) {
	if repo == nil {
		return
	}
	e.signals = repo
}

// SignalRepository returns the repository Detect currently persists
// to. Used by cmd/chronos to wrap the store with webhook notifiers.
func (e *Engine) SignalRepository() ports.SignalRepository {
	return e.signals
}

// Process persists a single observation. The state is validated and
// stored via the engine's EntityStateRepository. Detection is NOT run
// automatically; call [Engine.Detect] or [Engine.DetectStates] to
// surface the signals the new state implies.
func (e *Engine) Process(ctx context.Context, state chronos.EntityState) error {
	if err := state.Validate(); err != nil {
		return fmt.Errorf("chronos/embed: invalid state: %w", err)
	}
	return e.conn.EntityStates.Ingest(ctx, e.adapterName, state)
}

// ProcessBatch persists a batch of observations atomically (when the
// underlying provider supports transactions) or sequentially (when it
// does not). All states are validated before any write.
func (e *Engine) ProcessBatch(ctx context.Context, states []chronos.EntityState) error {
	for i, s := range states {
		if err := s.Validate(); err != nil {
			return fmt.Errorf("chronos/embed: invalid state at index %d: %w", i, err)
		}
	}
	return e.conn.EntityStates.Save(ctx, e.adapterName, states)
}

// Detect runs the detector set against the entity states currently
// stored under each of the supplied scope ids. Detected signals are
// persisted and returned. Pass an empty slice to skip detection.
func (e *Engine) Detect(ctx context.Context, scopeIDs []uuid.UUID) ([]chronos.Signal, error) {
	if len(scopeIDs) == 0 {
		return nil, nil
	}

	var states []chronos.EntityState
	for _, sid := range scopeIDs {
		scoped, err := e.conn.EntityStates.ListByScope(ctx, sid)
		if err != nil {
			return nil, fmt.Errorf("chronos/embed: list states for scope %s: %w", sid, err)
		}
		states = append(states, scoped...)
	}
	return e.DetectStates(ctx, states)
}

// DetectStates runs detectors on the supplied observations (without
// re-loading from storage) and persists any emitted signals. This is
// the batch-only path cmd/chronos compute uses so a fetch-and-detect
// run analyses the adapter payload rather than the whole scope history.
func (e *Engine) DetectStates(ctx context.Context, states []chronos.EntityState) ([]chronos.Signal, error) {
	if len(states) == 0 {
		return nil, nil
	}
	signals := e.detector.Detect(ctx, states)
	for _, sig := range signals {
		if err := e.signals.Save(ctx, sig); err != nil {
			return signals, fmt.Errorf("chronos/embed: persist signal %s: %w", sig.ID, err)
		}
	}
	return signals, nil
}

// Query fetches stored signals matching the supplied options. Results
// are ordered detected-at descending (then confidence descending) per
// the SignalRepository contract.
func (e *Engine) Query(ctx context.Context, opts QueryOpts) ([]chronos.Signal, error) {
	filter := ports.SignalFilter{
		ScopeID:       opts.ScopeID,
		ScopeIDs:      opts.ScopeIDs,
		Series:        opts.Series,
		Pattern:       opts.Pattern,
		Since:         opts.Since,
		Until:         opts.Until,
		MinConfidence: opts.MinConfidence,
		Limit:         opts.Limit,
	}
	signals, err := e.signals.List(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("chronos/embed: list signals: %w", err)
	}
	return signals, nil
}

// Close releases the engine's storage handle. Idempotent — safe to call
// more than once.
func (e *Engine) Close() error {
	if e == nil || e.conn == nil {
		return nil
	}
	err := e.conn.Close()
	e.conn = nil
	return err
}
