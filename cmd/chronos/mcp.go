// Package main — chronos mcp subcommand.
//
// Exposes Chronos's perception primitives over the Model Context
// Protocol (stdio transport) so MCP-aware hosts (Claude Code, Letta,
// Anthropic Desktop) discover signal queries, observation ingest, and
// detector descriptions natively — no per-host adapter required.
//
// Three tools today:
//
//	list_signals       — query detected patterns by scope / pattern
//	                     / since / min_confidence
//	ingest             — submit one observation (EntityState)
//	describe_detector  — return a detector's thresholds + enablement
//	                     status under the current config
//
// The tool set is intentionally small. MCP hosts compose these with
// their own narration; Chronos stays signals-only.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/felixgeelhaar/chronos/internal/api"
	"github.com/felixgeelhaar/chronos/internal/config"
	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/felixgeelhaar/chronos/internal/ports"
	"github.com/felixgeelhaar/chronos/internal/store"
	"github.com/google/uuid"
	mcp "go.klarlabs.de/mcp"
)

// mcpListSignalsInput mirrors the HTTP /v1/signals filter knobs.
// scope_id is required for the same reason it is on the HTTP path —
// signal listing without a scope filter is the firehose, dangerous in
// any multi-tenant deployment.
type mcpListSignalsInput struct {
	ScopeID       string   `json:"scope_id,omitempty" jsonschema:"description=Single scope to query. Mutually exclusive with scope_ids."`
	ScopeIDs      []string `json:"scope_ids,omitempty" jsonschema:"description=Allowlist of scopes (multi-tenant read). Mutually exclusive with scope_id."`
	Pattern       string   `json:"pattern,omitempty" jsonschema:"description=Restrict to one pattern type (e.g. trend, recurrence)"`
	Since         string   `json:"since,omitempty" jsonschema:"description=RFC3339 lower bound on detected_at"`
	MinConfidence float64  `json:"min_confidence,omitempty" jsonschema:"description=Drop signals below this confidence (0..1)"`
	Limit         int      `json:"limit,omitempty" jsonschema:"description=Max signals to return (0 = no limit)"`
}

type mcpListSignalsOutput struct {
	Signals []api.SignalDTO `json:"signals"`
	Count   int             `json:"count"`
}

func mcpRunListSignals(ctx context.Context, input mcpListSignalsInput) (mcpListSignalsOutput, error) {
	if input.ScopeID == "" && len(input.ScopeIDs) == 0 {
		return mcpListSignalsOutput{}, fmt.Errorf("scope_id or scope_ids required")
	}
	if input.ScopeID != "" && len(input.ScopeIDs) > 0 {
		return mcpListSignalsOutput{}, fmt.Errorf("set scope_id OR scope_ids, not both")
	}
	filter := ports.SignalFilter{}
	if input.ScopeID != "" {
		s, err := uuid.Parse(input.ScopeID)
		if err != nil {
			return mcpListSignalsOutput{}, fmt.Errorf("invalid scope_id: %w", err)
		}
		filter.ScopeID = s
	}
	if len(input.ScopeIDs) > 0 {
		for _, raw := range input.ScopeIDs {
			id, err := uuid.Parse(raw)
			if err != nil {
				return mcpListSignalsOutput{}, fmt.Errorf("invalid scope_ids entry %q: %w", raw, err)
			}
			filter.ScopeIDs = append(filter.ScopeIDs, id)
		}
	}
	if input.Pattern != "" {
		p := domain.PatternType(input.Pattern)
		filter.Pattern = &p
	}
	if input.Since != "" {
		t, err := time.Parse(time.RFC3339, input.Since)
		if err != nil {
			return mcpListSignalsOutput{}, fmt.Errorf("invalid since: %w", err)
		}
		filter.Since = &t
	}
	if input.MinConfidence > 0 {
		mc := input.MinConfidence
		filter.MinConfidence = &mc
	}
	if input.Limit > 0 {
		filter.Limit = input.Limit
	}

	conn, err := openMCPConn(ctx)
	if err != nil {
		return mcpListSignalsOutput{}, err
	}
	defer func() { _ = conn.Close() }()

	signals, err := conn.Signals.List(ctx, filter)
	if err != nil {
		return mcpListSignalsOutput{}, fmt.Errorf("list signals: %w", err)
	}
	dtos := make([]api.SignalDTO, 0, len(signals))
	for _, sig := range signals {
		dtos = append(dtos, api.ToSignalDTO(sig))
	}
	return mcpListSignalsOutput{Signals: dtos, Count: len(dtos)}, nil
}

type mcpIngestInput struct {
	EntityID  string            `json:"entity_id" jsonschema:"required,description=The entity (series) the observation belongs to"`
	ScopeID   string            `json:"scope_id" jsonschema:"required,description=Tenant scope"`
	Timestamp string            `json:"timestamp,omitempty" jsonschema:"description=RFC3339 timestamp (defaults to now)"`
	Features  []float64         `json:"features" jsonschema:"required,description=Observation feature vector"`
	Labels    []string          `json:"labels,omitempty" jsonschema:"description=Feature label names"`
	Meta      map[string]string `json:"meta,omitempty" jsonschema:"description=Adapter metadata"`
	Adapter   string            `json:"adapter,omitempty" jsonschema:"description=Source label; defaults to mcp"`
}

type mcpIngestOutput struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}

func mcpRunIngest(ctx context.Context, input mcpIngestInput) (mcpIngestOutput, error) {
	if input.EntityID == "" || input.ScopeID == "" {
		return mcpIngestOutput{}, fmt.Errorf("entity_id and scope_id are required")
	}
	if len(input.Features) == 0 {
		return mcpIngestOutput{}, fmt.Errorf("features required")
	}
	entityID, err := uuid.Parse(input.EntityID)
	if err != nil {
		return mcpIngestOutput{}, fmt.Errorf("invalid entity_id: %w", err)
	}
	scopeID, err := uuid.Parse(input.ScopeID)
	if err != nil {
		return mcpIngestOutput{}, fmt.Errorf("invalid scope_id: %w", err)
	}
	ts := time.Now().UTC()
	if input.Timestamp != "" {
		t, err := time.Parse(time.RFC3339, input.Timestamp)
		if err != nil {
			return mcpIngestOutput{}, fmt.Errorf("invalid timestamp: %w", err)
		}
		ts = t
	}
	adapter := input.Adapter
	if adapter == "" {
		adapter = "mcp"
	}
	state := chronos.EntityState{
		ID:        uuid.New(),
		EntityID:  entityID,
		ScopeID:   scopeID,
		Timestamp: ts,
		Features:  input.Features,
		Labels:    input.Labels,
		Meta:      input.Meta,
	}
	if err := state.Validate(); err != nil {
		return mcpIngestOutput{}, fmt.Errorf("validate: %w", err)
	}
	conn, err := openMCPConn(ctx)
	if err != nil {
		return mcpIngestOutput{}, err
	}
	defer func() { _ = conn.Close() }()
	if err := conn.EntityStates.Ingest(ctx, adapter, state); err != nil {
		return mcpIngestOutput{}, fmt.Errorf("ingest: %w", err)
	}
	return mcpIngestOutput{ID: state.ID.String(), Status: "accepted"}, nil
}

type mcpDescribeDetectorInput struct {
	Pattern string `json:"pattern" jsonschema:"required,description=Pattern type: trend, spike, drop, stall, anomaly, recurrence, correlation, change_point, outlier_cluster, cross_scope_correlation, seasonality, oscillation, divergence, convergence"`
}

type mcpDescribeDetectorOutput struct {
	Pattern    string         `json:"pattern"`
	Enabled    bool           `json:"enabled"`
	Reason     string         `json:"reason,omitempty"`
	Thresholds map[string]any `json:"thresholds"`
	Warnings   []string       `json:"warnings,omitempty"`
}

func mcpRunDescribeDetector(_ context.Context, input mcpDescribeDetectorInput) (mcpDescribeDetectorOutput, error) {
	if input.Pattern == "" {
		return mcpDescribeDetectorOutput{}, fmt.Errorf("pattern required")
	}
	cfg := config.Default()
	all := api.BuildConfigReportForExport(cfg)
	for _, r := range all {
		if r.Name == input.Pattern {
			return mcpDescribeDetectorOutput{
				Pattern:    r.Name,
				Enabled:    r.Enabled,
				Reason:     r.Reason,
				Thresholds: r.Thresholds,
				Warnings:   r.Warnings,
			}, nil
		}
	}
	return mcpDescribeDetectorOutput{}, fmt.Errorf("unknown pattern %q", input.Pattern)
}

// openMCPConn opens a store connection through the same DSN-resolution
// path the serve subcommand uses, so the MCP tools see the same data
// the HTTP API sees.
func openMCPConn(ctx context.Context) (*store.Conn, error) {
	cfg := config.Default()
	dsn, err := resolveDSN(cfg)
	if err != nil {
		return nil, err
	}
	return store.Open(ctx, dsn)
}

// runMCP is the cmd/chronos entrypoint for `chronos mcp`. Wires the
// stdio MCP server, registers the three tools, and blocks until
// SIGINT/SIGTERM cancels the parent context.
func runMCP(_ []string) error {
	srv := mcp.NewServer(mcp.ServerInfo{
		Name:    "chronos",
		Version: version,
		Capabilities: mcp.Capabilities{
			Tools: true,
		},
	},
		mcp.WithTitle("Chronos MCP Server"),
		mcp.WithDescription("Query detected patterns, ingest observations, and inspect detector configuration."),
		mcp.WithWebsiteURL("https://github.com/felixgeelhaar/chronos"),
		mcp.WithInstructions("Use list_signals to query patterns, ingest to submit observations, describe_detector to inspect a detector's thresholds + enablement under the current config."),
	)

	srv.Tool("list_signals").
		Description("Return detected patterns matching the filter. scope_id (or scope_ids) is required.").
		OutputSchema(mcpListSignalsOutput{}).
		Handler(mcpRunListSignals)

	srv.Tool("ingest").
		Description("Persist a single time-series observation (EntityState) for downstream detection.").
		OutputSchema(mcpIngestOutput{}).
		Handler(mcpRunIngest)

	srv.Tool("describe_detector").
		Description("Return a detector's tuning thresholds and whether it is enabled under the current config.").
		OutputSchema(mcpDescribeDetectorOutput{}).
		Handler(mcpRunDescribeDetector)

	rootCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stopSignals := make(chan os.Signal, 1)
	signal.Notify(stopSignals, os.Interrupt, syscall.SIGTERM)
	go func() {
		sig, ok := <-stopSignals
		if !ok {
			return
		}
		fmt.Fprintf(os.Stderr, "chronos mcp: received %s, shutting down...\n", sig)
		cancel()
	}()

	if err := mcp.ServeStdio(rootCtx, srv); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	return nil
}

// jsonDump is a tiny convenience used by tests / debug paths that
// want a deterministic stringification of an output value.
func jsonDump(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}
