// Package grpc provides Chronos's gRPC transport layer. It mirrors the HTTP
// REST surface: ingest observations and query signals. The gRPC server shares
// the same domain ports (EntityStateRepository, SignalRepository) as the HTTP
// server; it is a thin transport adapter only.
package grpc

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/felixgeelhaar/chronos"
	chronosv1 "github.com/felixgeelhaar/chronos/api/proto/chronos/v1"
	httapi "github.com/felixgeelhaar/chronos/internal/api"
	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/felixgeelhaar/chronos/internal/observability"
	"github.com/felixgeelhaar/chronos/internal/ports"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server implements chronosv1.ChronosServiceServer.
type Server struct {
	chronosv1.UnimplementedChronosServiceServer

	states   ports.EntityStateRepository
	signals  ports.SignalRepository
	metrics  *observability.Metrics
	logger   *slog.Logger
	streamer SignalStreamer
}

// SignalStreamer is the gRPC equivalent of api.SSEBroadcaster.
// notify.SSE satisfies it.
type SignalStreamer interface {
	Subscribe(scopes []uuid.UUID, pattern string) (uuid.UUID, <-chan domain.Signal)
	Unsubscribe(uuid.UUID)
}

// NewServer wires a gRPC server. Pass slog.Default() for the logger if the
// caller has no preference. metrics may be nil.
func NewServer(states ports.EntityStateRepository, signals ports.SignalRepository, metrics *observability.Metrics, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{states: states, signals: signals, metrics: metrics, logger: logger}
}

// WithStreamer attaches a live signal broadcaster so StreamSignals
// can subscribe. Without it the RPC returns Unimplemented.
func (s *Server) WithStreamer(st SignalStreamer) *Server {
	s.streamer = st
	return s
}

// Ingest persists a single EntityState observation.
func (s *Server) Ingest(ctx context.Context, req *chronosv1.IngestRequest) (*chronosv1.IngestResponse, error) {
	if s.states == nil {
		return nil, status.Error(codes.Unimplemented, "ingest disabled")
	}

	state, err := ingestRequestToEntityState(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	adapter := req.Adapter
	if adapter == "" {
		adapter = "grpc"
	}

	if err := s.states.Ingest(ctx, adapter, state); err != nil {
		s.logger.Error("grpc ingest failed", "err", err)
		return nil, status.Error(codes.Internal, "ingest failed")
	}

	if s.metrics != nil {
		s.metrics.ObserveObservations(adapter, 1)
	}

	return &chronosv1.IngestResponse{
		Id:     state.ID.String(),
		Status: "accepted",
	}, nil
}

// ListSignals returns signals matching the provided filter.
func (s *Server) ListSignals(ctx context.Context, req *chronosv1.ListSignalsRequest) (*chronosv1.ListSignalsResponse, error) {
	filter, err := parseListSignalsRequest(req)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	if req.SinceCursor != "" {
		cursorAt, cursorID, err := httapi.DecodeListCursor(req.SinceCursor)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid since_cursor: "+err.Error())
		}
		filter.After = &ports.SignalCursor{DetectedAt: cursorAt, ID: cursorID}
	}
	// Same bounded, oldest-first paging as HTTP. A full page (count ==
	// the request limit, or 500 when none was sent) means more remain.
	httapi.PageForward(&filter)

	sigs, err := s.signals.List(ctx, filter)
	if err != nil {
		s.logger.Error("grpc list signals failed", "err", err)
		return nil, status.Error(codes.Internal, "list signals failed")
	}

	out := make([]*chronosv1.Signal, 0, len(sigs))
	for _, sig := range sigs {
		out = append(out, fromDomainSignal(sig))
	}

	resp := &chronosv1.ListSignalsResponse{
		Signals: out,
		Count:   int32(len(out)), //nolint:gosec // bounded by query limit; no overflow risk
	}
	if next, ok := httapi.NextCursor(filter, sigs); ok {
		resp.NextCursor = httapi.EncodeListCursor(next.DetectedAt, next.ID)
	}
	return resp, nil
}

// GetSignal returns a single signal by ID.
func (s *Server) GetSignal(ctx context.Context, req *chronosv1.GetSignalRequest) (*chronosv1.Signal, error) {
	id, err := uuid.Parse(req.Id)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid signal id")
	}

	sig, err := s.signals.Get(ctx, id)
	if errors.Is(err, domain.ErrSignalNotFound) {
		return nil, status.Error(codes.NotFound, "signal not found")
	}
	if err != nil {
		s.logger.Error("grpc get signal failed", "id", id, "err", err)
		return nil, status.Error(codes.Internal, "get signal failed")
	}

	return fromDomainSignal(sig), nil
}

// ingestRequestToEntityState converts a proto IngestRequest into a validated
// chronos.EntityState after applying defaults.
func ingestRequestToEntityState(req *chronosv1.IngestRequest) (chronos.EntityState, error) {
	if req.EntityId == "" {
		return chronos.EntityState{}, errors.New("entity_id required")
	}
	if req.ScopeId == "" {
		return chronos.EntityState{}, errors.New("scope_id required")
	}
	if len(req.Features) == 0 {
		return chronos.EntityState{}, errors.New("features required")
	}

	ts := protoTime(req.Timestamp)
	if ts.IsZero() {
		ts = time.Now().UTC()
	}

	id := parseUUID(req.Id)
	if id == uuid.Nil {
		id = uuid.New()
	}

	state := chronos.EntityState{
		ID:        id,
		EntityID:  parseUUID(req.EntityId),
		ScopeID:   parseUUID(req.ScopeId),
		Timestamp: ts,
		Features:  req.Features,
		Labels:    req.Labels,
		Meta:      req.Meta,
	}
	if err := state.Validate(); err != nil {
		return chronos.EntityState{}, err
	}
	return state, nil
}

// parseListSignalsRequest converts a proto ListSignalsRequest into a
// ports.SignalFilter. At least one of scope_id or scope_ids must be set
// (server-enforced tenant boundary; matches the HTTP contract).
func parseListSignalsRequest(req *chronosv1.ListSignalsRequest) (ports.SignalFilter, error) {
	if req.ScopeId == "" && len(req.ScopeIds) == 0 {
		return ports.SignalFilter{}, errors.New("scope_id or scope_ids required")
	}

	f := ports.SignalFilter{}
	if req.ScopeId != "" {
		scopeID, err := uuid.Parse(req.ScopeId)
		if err != nil {
			return ports.SignalFilter{}, errors.New("invalid scope_id")
		}
		f.ScopeID = scopeID
	}
	if len(req.ScopeIds) > 0 {
		f.ScopeIDs = make([]uuid.UUID, 0, len(req.ScopeIds))
		for _, raw := range req.ScopeIds {
			id, err := uuid.Parse(raw)
			if err != nil {
				return ports.SignalFilter{}, errors.New("invalid scope_ids entry")
			}
			f.ScopeIDs = append(f.ScopeIDs, id)
		}
	}

	if req.Series != "" {
		s, err := uuid.Parse(req.Series)
		if err != nil {
			return ports.SignalFilter{}, errors.New("invalid series")
		}
		f.Series = &s
	}
	if req.Pattern != chronosv1.PatternType_PATTERN_TYPE_UNSPECIFIED {
		p := patternTypeToDomain(req.Pattern)
		f.Pattern = &p
	}
	if req.Since != nil {
		t := req.Since.AsTime()
		f.Since = &t
	}
	if req.Until != nil {
		t := req.Until.AsTime()
		f.Until = &t
	}
	if req.MinConfidence > 0 {
		f.MinConfidence = &req.MinConfidence
	}
	if req.Limit > 0 {
		f.Limit = int(req.Limit)
	}
	return f, nil
}
