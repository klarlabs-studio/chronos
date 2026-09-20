package client

import (
	"context"
	"errors"
	"fmt"
	"time"

	chronosv1 "github.com/felixgeelhaar/chronos/api/proto/chronos/v1"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// GRPCClient is a gRPC transport implementation of the Chronos client
// surface. It returns the same Signal and IngestRequest types as the HTTP
// [Client] so callers can switch transports without changing business code.
type GRPCClient struct {
	conn   *grpc.ClientConn
	client chronosv1.ChronosServiceClient
	token  string
}

// GRPCOption configures a GRPCClient.
type GRPCOption func(*GRPCClient)

// WithGRPCToken sets a bearer token sent as gRPC metadata on every RPC.
func WithGRPCToken(tok string) GRPCOption { return func(c *GRPCClient) { c.token = tok } }

// NewGRPC constructs a GRPCClient targeting addr (e.g. "localhost:7779").
func NewGRPC(addr string, opts ...GRPCOption) (*GRPCClient, error) {
	if addr == "" {
		return nil, errors.New("chronos grpc client: address required")
	}
	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(16<<20)), // 16 MiB
	)
	if err != nil {
		return nil, fmt.Errorf("chronos grpc client: dial: %w", err)
	}
	c := &GRPCClient{
		conn:   conn,
		client: chronosv1.NewChronosServiceClient(conn),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c, nil
}

// Close tears down the underlying gRPC connection.
func (c *GRPCClient) Close() error { return c.conn.Close() }

// Ingest sends a single observation to the gRPC Ingest RPC.
func (c *GRPCClient) Ingest(ctx context.Context, req IngestRequest) (uuid.UUID, error) {
	protoReq := &chronosv1.IngestRequest{
		Id:       req.ID.String(),
		EntityId: req.EntityID.String(),
		ScopeId:  req.ScopeID.String(),
		Features: req.Features,
		Labels:   req.Labels,
		Meta:     req.Meta,
		Adapter:  req.Adapter,
	}
	if !req.Timestamp.IsZero() {
		protoReq.Timestamp = timestamppb.New(req.Timestamp)
	}

	resp, err := c.client.Ingest(c.withAuth(ctx), protoReq)
	if err != nil {
		return uuid.Nil, fmt.Errorf("chronos grpc client: ingest: %w", err)
	}
	id, err := uuid.Parse(resp.Id)
	if err != nil {
		return uuid.Nil, fmt.Errorf("chronos grpc client: parse response id: %w", err)
	}
	return id, nil
}

// IngestBatch persists many observations via the IngestBatch RPC.
func (c *GRPCClient) IngestBatch(ctx context.Context, observations []IngestRequest) (IngestBatchResponse, error) {
	obs := make([]*chronosv1.IngestRequest, 0, len(observations))
	for _, req := range observations {
		p := &chronosv1.IngestRequest{
			Id:       req.ID.String(),
			EntityId: req.EntityID.String(),
			ScopeId:  req.ScopeID.String(),
			Features: req.Features,
			Labels:   req.Labels,
			Meta:     req.Meta,
			Adapter:  req.Adapter,
		}
		if !req.Timestamp.IsZero() {
			p.Timestamp = timestamppb.New(req.Timestamp)
		}
		obs = append(obs, p)
	}
	resp, err := c.client.IngestBatch(c.withAuth(ctx), &chronosv1.IngestBatchRequest{Observations: obs})
	if err != nil {
		return IngestBatchResponse{}, fmt.Errorf("chronos grpc client: ingest batch: %w", err)
	}
	return IngestBatchResponse{Accepted: int(resp.Accepted), DeferDetection: resp.DeferDetection}, nil
}

// FederationExport pulls the anonymized pattern-statistics payload.
func (c *GRPCClient) FederationExport(ctx context.Context) (FederationExport, error) {
	resp, err := c.client.ExportFederation(c.withAuth(ctx), &chronosv1.ExportFederationRequest{})
	if err != nil {
		return FederationExport{}, fmt.Errorf("chronos grpc client: federation export: %w", err)
	}
	patterns := make([]FederationPatternStats, 0, len(resp.Patterns))
	for _, p := range resp.Patterns {
		if p == nil {
			continue
		}
		patterns = append(patterns, FederationPatternStats{
			Pattern:          p.Pattern,
			Count:            int(p.Count),
			AvgStrength:      p.AvgStrength,
			MinStrength:      p.MinStrength,
			MaxStrength:      p.MaxStrength,
			AvgConfidence:    p.AvgConfidence,
			MinConfidence:    p.MinConfidence,
			MaxConfidence:    p.MaxConfidence,
			AvgSampleSize:    p.AvgSampleSize,
			TentativeCount:   int(p.TentativeCount),
			EstablishedCount: int(p.EstablishedCount),
			StrongCount:      int(p.StrongCount),
		})
	}
	out := FederationExport{
		Source:       resp.Source,
		Version:      resp.Version,
		Patterns:     patterns,
		TotalSignals: int(resp.TotalSignals),
	}
	if resp.GeneratedAt != nil {
		out.GeneratedAt = resp.GeneratedAt.AsTime()
	}
	return out, nil
}

// ValidateConfig dry-runs a candidate env-var map.
func (c *GRPCClient) ValidateConfig(ctx context.Context, env map[string]string) ([]DetectorReport, error) {
	resp, err := c.client.ValidateConfig(c.withAuth(ctx), &chronosv1.ValidateConfigRequest{Env: env})
	if err != nil {
		return nil, fmt.Errorf("chronos grpc client: validate config: %w", err)
	}
	out := make([]DetectorReport, 0, len(resp.Detectors))
	for _, r := range resp.Detectors {
		if r == nil {
			continue
		}
		out = append(out, DetectorReport{
			Name:       r.Name,
			Enabled:    r.Enabled,
			Reason:     r.Reason,
			Thresholds: r.Thresholds,
			Warnings:   r.Warnings,
		})
	}
	return out, nil
}

// Signals returns a fluent builder for the ListSignals RPC.
func (c *GRPCClient) Signals() *GRPCSignalQuery {
	return &GRPCSignalQuery{c: c}
}

// GRPCSignalQuery is the gRPC equivalent of SignalQuery.
type GRPCSignalQuery struct {
	c             *GRPCClient
	scope         uuid.UUID
	scopes        []uuid.UUID
	series        *uuid.UUID
	pattern       *string
	since, until  *time.Time
	minConfidence *float64
	limit         int
	sinceCursor   string
}

// Scope filters by scope ID. Required for List unless Scopes is set.
func (q *GRPCSignalQuery) Scope(id uuid.UUID) *GRPCSignalQuery {
	q.scope = id
	return q
}

// Scopes sets a multi-scope allowlist (proto scope_ids).
func (q *GRPCSignalQuery) Scopes(ids ...uuid.UUID) *GRPCSignalQuery {
	q.scopes = ids
	return q
}

// Series filters to signals about a single entity.
func (q *GRPCSignalQuery) Series(id uuid.UUID) *GRPCSignalQuery {
	q.series = &id
	return q
}

// Pattern filters to a single PatternType (use the PatternType* constants).
func (q *GRPCSignalQuery) Pattern(name string) *GRPCSignalQuery {
	q.pattern = &name
	return q
}

// Since restricts results to signals detected at or after t.
func (q *GRPCSignalQuery) Since(t time.Time) *GRPCSignalQuery {
	q.since = &t
	return q
}

// Until restricts results to signals detected before t.
func (q *GRPCSignalQuery) Until(t time.Time) *GRPCSignalQuery {
	q.until = &t
	return q
}

// MinConfidence drops signals below threshold.
func (q *GRPCSignalQuery) MinConfidence(threshold float64) *GRPCSignalQuery {
	q.minConfidence = &threshold
	return q
}

// Limit caps the number of returned signals.
func (q *GRPCSignalQuery) Limit(n int) *GRPCSignalQuery {
	q.limit = n
	return q
}

// SinceCursor resumes a List after the opaque next_cursor token.
func (q *GRPCSignalQuery) SinceCursor(token string) *GRPCSignalQuery {
	q.sinceCursor = token
	return q
}

func (q *GRPCSignalQuery) listRequest() (*chronosv1.ListSignalsRequest, error) {
	if q.scope == uuid.Nil && len(q.scopes) == 0 {
		return nil, errors.New("chronos grpc client: Scope or Scopes is required")
	}
	if q.scope != uuid.Nil && len(q.scopes) > 0 {
		return nil, errors.New("chronos grpc client: set Scope or Scopes, not both")
	}
	req := &chronosv1.ListSignalsRequest{
		Limit:       int32(q.limit), //nolint:gosec
		SinceCursor: q.sinceCursor,
	}
	if q.scope != uuid.Nil {
		req.ScopeId = q.scope.String()
	}
	if len(q.scopes) > 0 {
		req.ScopeIds = make([]string, len(q.scopes))
		for i, id := range q.scopes {
			req.ScopeIds[i] = id.String()
		}
	}
	if q.series != nil {
		req.Series = q.series.String()
	}
	if q.pattern != nil {
		req.Pattern = patternTypeFromString(*q.pattern)
	}
	if q.since != nil {
		req.Since = timestamppb.New(*q.since)
	}
	if q.until != nil {
		req.Until = timestamppb.New(*q.until)
	}
	if q.minConfidence != nil {
		req.MinConfidence = *q.minConfidence
	}
	return req, nil
}

// List runs the query via the ListSignals RPC.
func (q *GRPCSignalQuery) List(ctx context.Context) ([]Signal, error) {
	page, err := q.ListPage(ctx)
	if err != nil {
		return nil, err
	}
	return page.Signals, nil
}

// ListPage returns signals plus the opaque next_cursor.
func (q *GRPCSignalQuery) ListPage(ctx context.Context) (SignalPage, error) {
	req, err := q.listRequest()
	if err != nil {
		return SignalPage{}, err
	}
	resp, err := q.c.client.ListSignals(q.c.withAuth(ctx), req)
	if err != nil {
		return SignalPage{}, fmt.Errorf("chronos grpc client: list signals: %w", err)
	}
	signals := make([]Signal, 0, len(resp.Signals))
	for _, ps := range resp.Signals {
		signals = append(signals, protoToSignal(ps))
	}
	return SignalPage{Signals: signals, Count: int(resp.Count), NextCursor: resp.NextCursor}, nil
}

// Stream subscribes to StreamSignals until ctx is cancelled.
func (q *GRPCSignalQuery) Stream(ctx context.Context) (<-chan Signal, error) {
	if q.scope == uuid.Nil && len(q.scopes) == 0 {
		return nil, errors.New("chronos grpc client: Scope or Scopes is required for Stream")
	}
	if q.scope != uuid.Nil && len(q.scopes) > 0 {
		return nil, errors.New("chronos grpc client: set Scope or Scopes, not both")
	}
	req := &chronosv1.StreamSignalsRequest{}
	if q.scope != uuid.Nil {
		req.ScopeId = q.scope.String()
	} else {
		req.ScopeIds = make([]string, len(q.scopes))
		for i, id := range q.scopes {
			req.ScopeIds[i] = id.String()
		}
	}
	if q.pattern != nil {
		req.Pattern = patternTypeFromString(*q.pattern)
	}
	stream, err := q.c.client.StreamSignals(q.c.withAuth(ctx), req)
	if err != nil {
		return nil, fmt.Errorf("chronos grpc client: stream signals: %w", err)
	}
	ch := make(chan Signal)
	go func() {
		defer close(ch)
		for {
			msg, err := stream.Recv()
			if err != nil {
				return
			}
			select {
			case <-ctx.Done():
				return
			case ch <- protoToSignal(msg.GetSignal()):
			}
		}
	}()
	return ch, nil
}

// Get returns a single signal by ID via the GetSignal RPC.
func (q *GRPCSignalQuery) Get(ctx context.Context, id uuid.UUID) (Signal, error) {
	resp, err := q.c.client.GetSignal(q.c.withAuth(ctx), &chronosv1.GetSignalRequest{Id: id.String()})
	if err != nil {
		return Signal{}, fmt.Errorf("chronos grpc client: get signal: %w", err)
	}
	return protoToSignal(resp), nil
}

// withAuth injects bearer token metadata when a token is configured.
func (c *GRPCClient) withAuth(ctx context.Context) context.Context {
	if c.token == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+c.token)
}

// patternTypeFromString maps a client PatternType constant to the proto enum.
func patternTypeFromString(s string) chronosv1.PatternType {
	switch s {
	case PatternTypeRecurrence:
		return chronosv1.PatternType_PATTERN_TYPE_RECURRENCE
	case PatternTypeTrend:
		return chronosv1.PatternType_PATTERN_TYPE_TREND
	case PatternTypeSpike:
		return chronosv1.PatternType_PATTERN_TYPE_SPIKE
	case PatternTypeDrop:
		return chronosv1.PatternType_PATTERN_TYPE_DROP
	case PatternTypeStall:
		return chronosv1.PatternType_PATTERN_TYPE_STALL
	case PatternTypeAnomaly:
		return chronosv1.PatternType_PATTERN_TYPE_ANOMALY
	case PatternTypeSeasonality:
		return chronosv1.PatternType_PATTERN_TYPE_SEASONALITY
	case PatternTypeCorrelation:
		return chronosv1.PatternType_PATTERN_TYPE_CORRELATION
	case PatternTypeChangePoint:
		return chronosv1.PatternType_PATTERN_TYPE_CHANGE_POINT
	case PatternTypeOutlierCluster:
		return chronosv1.PatternType_PATTERN_TYPE_OUTLIER_CLUSTER
	case PatternTypeCrossScopeCorrelation:
		return chronosv1.PatternType_PATTERN_TYPE_CROSS_SCOPE_CORRELATION
	case PatternTypeOscillation:
		return chronosv1.PatternType_PATTERN_TYPE_OSCILLATION
	case PatternTypeDivergence:
		return chronosv1.PatternType_PATTERN_TYPE_DIVERGENCE
	case PatternTypeConvergence:
		return chronosv1.PatternType_PATTERN_TYPE_CONVERGENCE
	default:
		return chronosv1.PatternType_PATTERN_TYPE_UNSPECIFIED
	}
}

// patternTypeToString maps a proto PatternType to the client PatternType constant.
func patternTypeToString(p chronosv1.PatternType) string {
	switch p {
	case chronosv1.PatternType_PATTERN_TYPE_RECURRENCE:
		return PatternTypeRecurrence
	case chronosv1.PatternType_PATTERN_TYPE_TREND:
		return PatternTypeTrend
	case chronosv1.PatternType_PATTERN_TYPE_SPIKE:
		return PatternTypeSpike
	case chronosv1.PatternType_PATTERN_TYPE_DROP:
		return PatternTypeDrop
	case chronosv1.PatternType_PATTERN_TYPE_STALL:
		return PatternTypeStall
	case chronosv1.PatternType_PATTERN_TYPE_ANOMALY:
		return PatternTypeAnomaly
	case chronosv1.PatternType_PATTERN_TYPE_SEASONALITY:
		return PatternTypeSeasonality
	case chronosv1.PatternType_PATTERN_TYPE_CORRELATION:
		return PatternTypeCorrelation
	case chronosv1.PatternType_PATTERN_TYPE_CHANGE_POINT:
		return PatternTypeChangePoint
	case chronosv1.PatternType_PATTERN_TYPE_OUTLIER_CLUSTER:
		return PatternTypeOutlierCluster
	case chronosv1.PatternType_PATTERN_TYPE_CROSS_SCOPE_CORRELATION:
		return PatternTypeCrossScopeCorrelation
	case chronosv1.PatternType_PATTERN_TYPE_OSCILLATION:
		return PatternTypeOscillation
	case chronosv1.PatternType_PATTERN_TYPE_DIVERGENCE:
		return PatternTypeDivergence
	case chronosv1.PatternType_PATTERN_TYPE_CONVERGENCE:
		return PatternTypeConvergence
	default:
		return ""
	}
}

// protoToSignal converts a proto Signal to the client.Signal wire type.
func protoToSignal(ps *chronosv1.Signal) Signal {
	if ps == nil {
		return Signal{}
	}
	evidence := make([]Evidence, 0, len(ps.Evidence))
	for _, pe := range ps.Evidence {
		if pe == nil {
			continue
		}
		evidence = append(evidence, Evidence{
			Series:  parseUUID(pe.Series),
			Time:    pe.Time.AsTime(),
			Kind:    pe.Kind,
			Score:   pe.Score,
			Metrics: pe.Metrics,
		})
	}
	return Signal{
		ID:         parseUUID(ps.Id),
		ScopeID:    parseUUID(ps.ScopeId),
		Series:     parseUUID(ps.Series),
		Pattern:    patternTypeToString(ps.Pattern),
		DetectedAt: ps.DetectedAt.AsTime(),
		Window: TimeWindow{
			Start: ps.Window.Start.AsTime(),
			End:   ps.Window.End.AsTime(),
		},
		Strength:        ps.Strength,
		Confidence:      ps.Confidence,
		ConfidenceClass: ps.ConfidenceClass,
		Metrics:         ps.Metrics,
		Evidence:        evidence,
		Explanation:     protoToExplanation(ps.Explanation),
	}
}

func protoToExplanation(e *chronosv1.Explanation) *Explanation {
	if e == nil {
		return nil
	}
	samples := make([]FeatureSample, 0, len(e.FeatureEvolution))
	for _, fs := range e.FeatureEvolution {
		if fs == nil {
			continue
		}
		samples = append(samples, FeatureSample{At: fs.At.AsTime(), Value: fs.Value})
	}
	return &Explanation{
		FeatureEvolution:   samples,
		ComparablePeers:    int(e.ComparablePeers),
		BaselineWindowDays: int(e.BaselineWindowDays),
		ThresholdUsed:      e.ThresholdUsed,
		DetectorVersion:    e.DetectorVersion,
	}
}

// parseUUID safely parses a UUID string; returns uuid.Nil on error.
func parseUUID(s string) uuid.UUID {
	id, err := uuid.Parse(s)
	if err != nil {
		return uuid.Nil
	}
	return id
}
