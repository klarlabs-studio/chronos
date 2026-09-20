package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/felixgeelhaar/chronos/internal/ports"
)

// FederationExportResponse is the wire shape for GET
// /v1/federation/export. Carries aggregated, anonymized pattern
// statistics: counts per pattern type, average / min / max strength
// and confidence, mean sample size, plus the generation timestamp.
//
// Nothing in the payload identifies a tenant. There are no scope_ids,
// no series ids, no signal ids, and no per-row evidence. That is the
// load-bearing safety property of the federation hook — the export
// is community-grade statistical perception, not raw data.
type FederationExportResponse struct {
	GeneratedAt  time.Time                `json:"generated_at"`
	Source       string                   `json:"source"`  // "chronos"
	Version      string                   `json:"version"` // schema version of this payload
	Patterns     []FederationPatternStats `json:"patterns"`
	TotalSignals int                      `json:"total_signals"`
}

// FederationPatternStats summarises one pattern type's signal
// population.
type FederationPatternStats struct {
	Pattern          string  `json:"pattern"`
	Count            int     `json:"count"`
	AvgStrength      float64 `json:"avg_strength"`
	MinStrength      float64 `json:"min_strength"`
	MaxStrength      float64 `json:"max_strength"`
	AvgConfidence    float64 `json:"avg_confidence"`
	MinConfidence    float64 `json:"min_confidence"`
	MaxConfidence    float64 `json:"max_confidence"`
	AvgSampleSize    float64 `json:"avg_sample_size,omitempty"` // mean of metrics["n"] when populated
	TentativeCount   int     `json:"tentative_count,omitempty"`
	EstablishedCount int     `json:"established_count,omitempty"`
	StrongCount      int     `json:"strong_count,omitempty"`
}

// FederationExportVersion is the wire-format generation. Bump on
// shape changes so downstream pull-mirror clients can reject
// incompatible payloads explicitly.
const FederationExportVersion = "v1"

// federationEnabled reports whether the federation export endpoint is
// allowed to serve. Opt-in via CHRONOS_FEDERATION_ENABLED=true.
// Default off — the issue's load-bearing requirement is "only when
// the operator says so".
func federationEnabled() bool {
	v := os.Getenv("CHRONOS_FEDERATION_ENABLED")
	if v == "" {
		return false
	}
	enabled, _ := strconv.ParseBool(v)
	return enabled
}

// FederationEnabled reports whether GET /v1/federation/export (and the
// matching gRPC RPC) is allowed to serve.
func FederationEnabled() bool { return federationEnabled() }

// handleFederationExport returns the aggregated pattern stats.
// Streams every signal in the store, buckets by pattern, computes
// the summary. For deployments with very large signal counts this
// would want pagination + sampling; today the workload is small and
// the math is cheap.
func (s *Server) handleFederationExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !federationEnabled() {
		http.Error(w, "federation export disabled (set CHRONOS_FEDERATION_ENABLED=true to opt in)", http.StatusNotImplemented)
		return
	}

	// List every signal across every scope. Federation is the one
	// path that legitimately spans scopes — the result is anonymized.
	signals, err := s.signals.List(r.Context(), ports.SignalFilter{})
	if err != nil {
		s.logger.Error("federation list signals failed", "err", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	stats := AggregateFederationStats(signals)
	respondJSON(w, http.StatusOK, FederationExportResponse{
		GeneratedAt:  time.Now().UTC(),
		Source:       "chronos",
		Version:      FederationExportVersion,
		Patterns:     stats,
		TotalSignals: len(signals),
	})
}

// AggregateFederationStats buckets signals by pattern type and
// computes per-bucket summary statistics. Exported so tests and the
// gRPC transport can assert against the same math as HTTP.
func AggregateFederationStats(signals []domain.Signal) []FederationPatternStats {
	type bucket struct {
		count                          int
		strSum, strMin, strMax         float64
		confSum, confMin, confMax      float64
		nSum                           float64
		nCount                         int
		tentative, established, strong int
	}
	buckets := map[domain.PatternType]*bucket{}
	for _, sig := range signals {
		b, ok := buckets[sig.Pattern]
		if !ok {
			b = &bucket{strMin: sig.Strength, strMax: sig.Strength, confMin: sig.Confidence, confMax: sig.Confidence}
			buckets[sig.Pattern] = b
		}
		b.count++
		b.strSum += sig.Strength
		b.confSum += sig.Confidence
		if sig.Strength < b.strMin {
			b.strMin = sig.Strength
		}
		if sig.Strength > b.strMax {
			b.strMax = sig.Strength
		}
		if sig.Confidence < b.confMin {
			b.confMin = sig.Confidence
		}
		if sig.Confidence > b.confMax {
			b.confMax = sig.Confidence
		}
		if n, ok := sig.Metrics["n"]; ok {
			b.nSum += n
			b.nCount++
		}
		switch sig.ConfidenceClass {
		case domain.ConfidenceClassTentative:
			b.tentative++
		case domain.ConfidenceClassEstablished:
			b.established++
		case domain.ConfidenceClassStrong:
			b.strong++
		}
	}
	out := make([]FederationPatternStats, 0, len(buckets))
	for pattern, b := range buckets {
		row := FederationPatternStats{
			Pattern:          string(pattern),
			Count:            b.count,
			AvgStrength:      b.strSum / float64(b.count),
			MinStrength:      b.strMin,
			MaxStrength:      b.strMax,
			AvgConfidence:    b.confSum / float64(b.count),
			MinConfidence:    b.confMin,
			MaxConfidence:    b.confMax,
			TentativeCount:   b.tentative,
			EstablishedCount: b.established,
			StrongCount:      b.strong,
		}
		if b.nCount > 0 {
			row.AvgSampleSize = b.nSum / float64(b.nCount)
		}
		out = append(out, row)
	}
	// Stable order by pattern name so two consecutive exports diff
	// cleanly under a federated registry.
	sortFederationStatsByPattern(out)
	return out
}

func sortFederationStatsByPattern(out []FederationPatternStats) {
	// Tiny insertion sort — typical population is < 12 pattern types,
	// not worth importing sort for the package noise.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j].Pattern < out[j-1].Pattern; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
}

// MarshalIndent is exported only so the cmd-line helper that pulls
// federation snapshots can render JSON with the same shape the wire
// uses. Kept here so the wire and the CLI never drift.
func MarshalIndent(v any) ([]byte, error) {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("marshal federation snapshot: %w", err)
	}
	return b, nil
}
