// Package api provides Chronos's HTTP REST API. It is a thin transport
// layer over the persistence ports: parse the request, call a port,
// render the response. Per the cognitive-stack vision Chronos surfaces
// signals; it does not interpret or render prose. The wire shape is
// just the structured signal — Title/Summary/Suggestion are not part
// of this API.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/felixgeelhaar/chronos/internal/observability"
	"github.com/felixgeelhaar/chronos/internal/ports"
	"github.com/google/uuid"
)

// SSEBroadcaster is the structural contract the api layer requires of
// any streaming source. internal/notify.SSE satisfies it without
// either package importing the other (notify -> api would otherwise
// cycle through webhook.go's use of ToSignalDTO).
type SSEBroadcaster interface {
	// Subscribe registers a stream client. scopes is a server-side
	// allowlist: nil delivers every scope (operator-grade reads),
	// non-nil restricts delivery to the listed scope ids. The handler
	// constructs the allowlist from ?scope_id= (single-element) or
	// ?scope_in= (multi-element) so clients can't widen their own
	// filter post-handshake.
	Subscribe(scopes []uuid.UUID, pattern string) (uuid.UUID, <-chan domain.Signal)
	Unsubscribe(uuid.UUID)
}

// Server holds dependencies for HTTP handlers.
type Server struct {
	states  ports.EntityStateRepository
	signals ports.SignalRepository
	metrics *observability.Metrics
	logger  *slog.Logger
	sse     SSEBroadcaster
}

// NewServer wires an HTTP server. Pass slog.Default() for the logger
// if the caller has no preference. The states repository is required
// to support the streaming /v1/ingest endpoint; pass nil to disable
// it (the route still mounts but returns 501). metrics may be nil,
// in which case /metrics returns an empty document and observations
// in handlers are no-ops.
func NewServer(states ports.EntityStateRepository, signals ports.SignalRepository, metrics *observability.Metrics, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{states: states, signals: signals, metrics: metrics, logger: logger}
}

// WithSSE attaches an SSE broadcaster so the /v1/signals/stream route
// can register subscribers. Returns the server for chaining. Without
// this attachment, /v1/signals/stream responds 501 Not Implemented.
func (s *Server) WithSSE(sse SSEBroadcaster) *Server {
	s.sse = sse
	return s
}

// RegisterRoutes wires API routes onto a ServeMux. Routes are stable
// wire contracts; bump /v1 to introduce incompatible changes.
func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/health", s.handleHealth)
	mux.HandleFunc("/metrics", s.handleMetrics)
	mux.HandleFunc("/v1/ingest", s.handleIngest)
	mux.HandleFunc("/v1/ingest/batch", s.handleIngestBatch)
	mux.HandleFunc("/v1/config/validate", s.handleConfigValidate)
	mux.HandleFunc("/v1/federation/export", s.handleFederationExport)
	mux.HandleFunc("/v1/signals", s.handleSignals)
	mux.HandleFunc("/v1/signals/stream", s.handleStream)
	mux.HandleFunc("/v1/signals/", s.handleSignalDetail)
}

func (s *Server) handleHealth(w http.ResponseWriter, _ *http.Request) {
	respondJSON(w, http.StatusOK, map[string]string{"status": "healthy", "service": "chronos"})
}

// handleMetrics renders the registry in Prometheus exposition
// format. When no registry is wired, the endpoint still returns 200
// with an empty body so scrape configs do not flap on misconfigured
// deployments.
func (s *Server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	if s.metrics == nil {
		return
	}
	if err := s.metrics.Render(w); err != nil {
		s.logger.Error("metrics render failed", "err", err)
	}
}

// handleIngest accepts a single TimeSeriesPoint / EntityState and
// persists it through the EntityStateRepository. Detection is not
// triggered automatically by ingest — the compute pipeline handles
// detection at job time.
func (s *Server) handleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.states == nil {
		http.Error(w, "ingest disabled", http.StatusNotImplemented)
		return
	}
	var body IngestRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	state, err := body.toEntityState()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	adapter := body.Adapter
	if adapter == "" {
		adapter = "http"
	}
	if err := s.states.Ingest(r.Context(), adapter, state); err != nil {
		s.logger.Error("ingest failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	s.metrics.ObserveObservations(adapter, 1)
	respondJSON(w, http.StatusAccepted, map[string]string{"status": "accepted", "id": state.ID.String()})
}

// handleIngestBatch accepts an array of observations in one POST and
// writes them via the repository's batch Save path. Validation is
// all-or-nothing: any one observation failing returns 400 without
// persisting any of them, so callers can retry deterministically.
//
// The DeferDetection flag is echoed but currently a no-op — chronos
// already separates ingest from detection (detection runs on its own
// schedule, never on ingest), so there is nothing to defer. The flag
// is part of the contract for forward compatibility with backends
// that might inline detection.
func (s *Server) handleIngestBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.states == nil {
		http.Error(w, "ingest disabled", http.StatusNotImplemented)
		return
	}
	var body IngestBatchRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		http.Error(w, "invalid body", http.StatusBadRequest)
		return
	}
	if len(body.Observations) == 0 {
		http.Error(w, "observations array is empty", http.StatusBadRequest)
		return
	}
	if len(body.Observations) > MaxIngestBatchSize {
		http.Error(w, fmt.Sprintf("batch size %d exceeds max %d", len(body.Observations), MaxIngestBatchSize), http.StatusBadRequest)
		return
	}

	// Group by adapter so the Save path stays single-call per adapter
	// (Save takes one adapter name and a slice). Most batches will be
	// from one source, so this is usually a single group.
	groups := make(map[string][]chronos.EntityState)
	for i, obs := range body.Observations {
		state, err := obs.toEntityState()
		if err != nil {
			http.Error(w, fmt.Sprintf("observations[%d]: %v", i, err), http.StatusBadRequest)
			return
		}
		adapter := obs.Adapter
		if adapter == "" {
			adapter = "http"
		}
		groups[adapter] = append(groups[adapter], state)
	}

	accepted := 0
	for adapter, states := range groups {
		if err := s.states.Save(r.Context(), adapter, states); err != nil {
			s.logger.Error("ingest batch failed", "adapter", adapter, "count", len(states), "err", err)
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		s.metrics.ObserveObservations(adapter, len(states))
		accepted += len(states)
	}
	respondJSON(w, http.StatusAccepted, IngestBatchResponse{
		Accepted:       accepted,
		DeferDetection: body.DeferDetection,
	})
}

// handleSignals lists signals matching a filter from the query string.
// All filters are optional except scope_id, which is required.
//
// Pagination: callers polling for "new since last check" pass the
// next_cursor token returned by their previous response as
// ?since_cursor=. The cursor encodes (detected_at, id), so equal
// timestamps tie-break on id and the client never has to trust its
// own clock.
func (s *Server) handleSignals(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	filter, err := parseSignalFilter(r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Decode the cursor before calling the repository so a malformed
	// token fails fast rather than running the query first.
	if token := r.URL.Query().Get("since_cursor"); token != "" {
		c, err := decodeSignalCursor(token)
		if err != nil {
			http.Error(w, "invalid since_cursor: "+err.Error(), http.StatusBadRequest)
			return
		}
		filter.After = &ports.SignalCursor{DetectedAt: c.DetectedAt, ID: c.ID}
	}
	pageForward(&filter)

	signals, err := s.signals.List(r.Context(), filter)
	if err != nil {
		s.logger.Error("list signals failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	dtos := make([]SignalDTO, 0, len(signals))
	for _, sig := range signals {
		dtos = append(dtos, ToSignalDTO(sig))
	}

	body := map[string]any{"signals": dtos, "count": len(dtos)}
	if cursorAt, ok := nextCursor(filter, signals); ok {
		body["next_cursor"] = encodeSignalCursor(signalCursor{DetectedAt: cursorAt.DetectedAt, ID: cursorAt.ID})
	}
	if filter.Forward() {
		body["has_more"] = len(signals) == filter.Limit
	}
	respondJSON(w, http.StatusOK, body)
}

// Forward pages (a since_cursor, or order=asc) are bounded even when the
// caller sends no limit. An unbounded cursor read is what took Vorhut's
// detection feed down on 2026-09-27: its cursor fell 48k signals behind,
// every poll asked for all of them and timed out, and a failed poll never
// advances a cursor.
const (
	defaultPageLimit = 500
	maxPageLimit     = 1000
)

// PageForward bounds a forward page; shared with the gRPC transport so
// both page identically.
func PageForward(f *ports.SignalFilter) { pageForward(f) }

// NextCursor is the shared resume position; see nextCursor.
func NextCursor(f ports.SignalFilter, signals []domain.Signal) (ports.SignalCursor, bool) {
	return nextCursor(f, signals)
}

func pageForward(f *ports.SignalFilter) {
	if !f.Forward() {
		return
	}
	if f.Limit <= 0 {
		f.Limit = defaultPageLimit
	}
	f.Limit = min(f.Limit, maxPageLimit)
}

// nextCursor is where the next poll resumes: after the last row of a
// forward page, or after the newest row of a newest-first list. It is
// absent for an empty page, so a caller keeps its position when nothing
// is new.
func nextCursor(f ports.SignalFilter, signals []domain.Signal) (ports.SignalCursor, bool) {
	if len(signals) == 0 {
		return ports.SignalCursor{}, false
	}
	at := signals[0]
	if f.Forward() {
		at = signals[len(signals)-1]
	}
	return ports.SignalCursor{DetectedAt: at.DetectedAt, ID: at.ID}, true
}

// handleStream is the Server-Sent Events feed for newly-detected
// signals. The connection is held open until the client disconnects
// or the request context is cancelled. Each emitted signal becomes a
// single SSE event:
//
//	id: <signal-uuid>
//	event: signal
//	data: {SignalDTO JSON}
//
// Filters: scope_id (required) and pattern (optional).
//
// Replay: clients may resume after a disconnect by re-issuing the
// request with the `Last-Event-ID` header set to the last signal ID
// they received (or the equivalent `last_event_id` query parameter,
// for environments where browsers strip the header). On reconnect
// the server replays signals detected at or after the referenced
// signal's detected_at, then continues with the live stream.
//
// When no broadcaster is attached the route returns 501 — the
// cmd/chronos `serve` binary attaches one whenever the detection
// scheduler is enabled (CHRONOS_DETECTION_INTERVAL > 0).
func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.sse == nil {
		http.Error(w, "streaming disabled", http.StatusNotImplemented)
		return
	}

	// Build the scope allowlist from either scope_id (single) or
	// scope_in (multi). One of the two must be set so a forgotten
	// query param can never produce an unfiltered firehose. Multi-
	// scope is the load-bearing tenant-safety feature: a per-user UI
	// listing N entities owned by one owner subscribes once with all
	// N scope ids and the server enforces the filter for the life of
	// the stream.
	scopeStr := strings.TrimSpace(r.URL.Query().Get("scope_id"))
	scopeInStr := strings.TrimSpace(r.URL.Query().Get("scope_in"))
	if scopeStr == "" && scopeInStr == "" {
		http.Error(w, "scope_id or scope_in required", http.StatusBadRequest)
		return
	}
	if scopeStr != "" && scopeInStr != "" {
		http.Error(w, "set scope_id OR scope_in, not both", http.StatusBadRequest)
		return
	}
	var scopes []uuid.UUID
	var primaryScope uuid.UUID // used by replay (single-scope path)
	if scopeStr != "" {
		s, err := uuid.Parse(scopeStr)
		if err != nil {
			http.Error(w, "invalid scope_id", http.StatusBadRequest)
			return
		}
		scopes = []uuid.UUID{s}
		primaryScope = s
	} else {
		for _, raw := range strings.Split(scopeInStr, ",") {
			raw = strings.TrimSpace(raw)
			if raw == "" {
				continue
			}
			id, err := uuid.Parse(raw)
			if err != nil {
				http.Error(w, "invalid scope_in entry: "+raw, http.StatusBadRequest)
				return
			}
			scopes = append(scopes, id)
		}
		if len(scopes) == 0 {
			http.Error(w, "scope_in must contain at least one uuid", http.StatusBadRequest)
			return
		}
		// Replay needs a single scope; with scope_in we replay only
		// the first listed scope (replay is non-fatal and conserved
		// for single-scope continuity).
		primaryScope = scopes[0]
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported by this server", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no") // disable nginx response buffering
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	pattern := r.URL.Query().Get("pattern")
	id, ch := s.sse.Subscribe(scopes, pattern)
	defer s.sse.Unsubscribe(id)

	// Send a comment line as the initial heartbeat so the client knows
	// the stream is established before the first signal arrives.
	_, _ = io.WriteString(w, ": connected\n\n")
	flusher.Flush()

	// Replay any signals missed since the client's last seen ID.
	if cursor := strings.TrimSpace(firstNonEmpty(r.Header.Get("Last-Event-ID"), r.URL.Query().Get("last_event_id"))); cursor != "" {
		if err := s.replaySinceCursor(r.Context(), w, flusher, primaryScope, pattern, cursor); err != nil {
			s.logger.Error("stream: replay failed", "err", err, "cursor", cursor)
			// Replay failure is non-fatal: keep the live stream going.
		}
	}

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case sig, open := <-ch:
			if !open {
				return // broadcaster shut down or unsubscribed
			}
			if err := writeSignalFrame(w, sig); err != nil {
				return // client disconnected
			}
			flusher.Flush()
		}
	}
}

// replaySinceCursor emits any signals detected at or after the
// timestamp of the cursor signal, scoped to the same filter as the
// live stream. Self-cursor row is excluded so consumers don't re-see
// the last event they already processed.
func (s *Server) replaySinceCursor(ctx context.Context, w io.Writer, flusher http.Flusher, scope uuid.UUID, pattern, cursor string) error {
	cursorID, err := uuid.Parse(cursor)
	if err != nil {
		return fmt.Errorf("parse cursor: %w", err)
	}
	cursorSig, err := s.signals.Get(ctx, cursorID)
	if err != nil {
		return fmt.Errorf("lookup cursor: %w", err)
	}
	since := cursorSig.DetectedAt
	filter := ports.SignalFilter{ScopeID: scope, Since: &since}
	if pattern != "" {
		pt := domain.PatternType(pattern)
		filter.Pattern = &pt
	}
	missed, err := s.signals.List(ctx, filter)
	if err != nil {
		return fmt.Errorf("list since cursor: %w", err)
	}
	for _, sig := range missed {
		if sig.ID == cursorID {
			continue // skip the row the client already saw
		}
		if err := writeSignalFrame(w, sig); err != nil {
			return err
		}
		flusher.Flush()
	}
	return nil
}

func writeSignalFrame(w io.Writer, sig domain.Signal) error {
	payload, err := json.Marshal(ToSignalDTO(sig))
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(w, "id: %s\nevent: signal\ndata: %s\n\n", sig.ID, payload)
	return err
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func (s *Server) handleSignalDetail(w http.ResponseWriter, r *http.Request) {
	idStr := strings.TrimPrefix(r.URL.Path, "/v1/signals/")
	id, err := uuid.Parse(idStr)
	if err != nil {
		http.Error(w, "invalid signal id", http.StatusBadRequest)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	sig, err := s.signals.Get(r.Context(), id)
	if errors.Is(err, domain.ErrSignalNotFound) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if err != nil {
		s.logger.Error("get signal failed", "id", id, "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	respondJSON(w, http.StatusOK, ToSignalDTO(sig))
}

func parseSignalFilter(r *http.Request) (ports.SignalFilter, error) {
	q := r.URL.Query()
	scopeIDStr := q.Get("scope_id")
	scopeInRaw := q.Get("scope_in")

	if scopeIDStr == "" && scopeInRaw == "" {
		return ports.SignalFilter{}, errors.New("scope_id or scope_in required")
	}

	f := ports.SignalFilter{}
	if scopeIDStr != "" {
		scopeID, err := uuid.Parse(scopeIDStr)
		if err != nil {
			return ports.SignalFilter{}, errors.New("invalid scope_id")
		}
		f.ScopeID = scopeID
	}
	if scopeInRaw != "" {
		ids := strings.Split(scopeInRaw, ",")
		f.ScopeIDs = make([]uuid.UUID, 0, len(ids))
		for _, raw := range ids {
			raw = strings.TrimSpace(raw)
			if raw == "" {
				continue
			}
			id, err := uuid.Parse(raw)
			if err != nil {
				return ports.SignalFilter{}, fmt.Errorf("invalid scope_in entry %q", raw)
			}
			f.ScopeIDs = append(f.ScopeIDs, id)
		}
		if len(f.ScopeIDs) == 0 {
			return ports.SignalFilter{}, errors.New("scope_in must contain at least one uuid")
		}
	}

	if v := q.Get("series"); v != "" {
		series, err := uuid.Parse(v)
		if err != nil {
			return ports.SignalFilter{}, errors.New("invalid series")
		}
		f.Series = &series
	}
	if v := q.Get("pattern"); v != "" {
		pat := domain.PatternType(v)
		f.Pattern = &pat
	}
	if v := q.Get("since"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return ports.SignalFilter{}, errors.New("invalid since")
		}
		f.Since = &t
	}
	if v := q.Get("until"); v != "" {
		t, err := time.Parse(time.RFC3339, v)
		if err != nil {
			return ports.SignalFilter{}, errors.New("invalid until")
		}
		f.Until = &t
	}
	if v := q.Get("min_confidence"); v != "" {
		conf, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return ports.SignalFilter{}, errors.New("invalid min_confidence")
		}
		f.MinConfidence = &conf
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return ports.SignalFilter{}, errors.New("invalid limit")
		}
		f.Limit = n
	}
	switch q.Get("order") {
	case "", "desc":
	case "asc":
		f.Ascending = true
	default:
		return ports.SignalFilter{}, errors.New("invalid order: want asc or desc")
	}
	return f, nil
}

// _ unused-import suppression — keeps chronos imported in case future
// helpers need it. (chronos.EntityState references live in dto.go.)
var _ = chronos.EntityState{}

func respondJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
