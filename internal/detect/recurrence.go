package detect

import (
	"context"
	"sort"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/felixgeelhaar/chronos/internal/config"
	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/felixgeelhaar/chronos/internal/similarity"
	"github.com/google/uuid"
)

// Recurrence detects PatternTypeRecurrence: situations where a subject
// entity is in a state that other entities in the same scope have been
// in before. Evidence comes from cross-entity cosine similarity over
// historical states.
//
// The detector is intentionally simple — it emits the *signal* that "we
// have seen this before"; what it means (good, bad, neutral) is the
// consumer's concern, not Chronos's.
type Recurrence struct {
	cfg *config.Config
	now func() time.Time
}

// NewRecurrence wires a Recurrence detector from configuration.
func NewRecurrence(cfg *config.Config) *Recurrence {
	return &Recurrence{cfg: cfg, now: time.Now}
}

// Pattern reports the PatternType this detector emits.
func (r *Recurrence) Pattern() domain.PatternType { return domain.PatternTypeRecurrence }

// Detect compares each entity's most recent state against historical
// states of *other* entities in the same scope. When at least
// MinSampleSize peers exceed SimilarityThreshold, a signal is emitted.
func (r *Recurrence) Detect(_ context.Context, scopeID uuid.UUID, states []chronos.EntityState) []domain.Signal {
	if len(states) < 2 {
		return nil
	}
	subjects := mostRecentByEntity(states)

	var signals []domain.Signal
	for _, entityID := range sortedUUIDKeys(subjects) {
		subject := subjects[entityID]
		evidence := r.gatherEvidence(entityID, subject, states)
		if len(evidence) == 0 || len(evidence) < r.cfg.MinSampleSize {
			continue
		}
		signals = append(signals, r.buildSignal(scopeID, subject, evidence))
	}
	return keepValid(signals)
}

func mostRecentByEntity(states []chronos.EntityState) map[uuid.UUID]chronos.EntityState {
	latest := make(map[uuid.UUID]chronos.EntityState)
	for _, s := range states {
		cur, ok := latest[s.EntityID]
		if !ok || afterByTimeThenID(s, cur) {
			latest[s.EntityID] = s
		}
	}
	return latest
}

// gatherEvidence finds peer states (different entity, strictly earlier
// in time) whose features are sufficiently similar to the subject.
// Length-mismatched or zero-norm vectors are skipped — cosine against
// them is undefined, not "dissimilar" (same fail-closed rule as Anomaly).
func (r *Recurrence) gatherEvidence(subjectID uuid.UUID, subject chronos.EntityState, all []chronos.EntityState) []domain.Evidence {
	if featureNormSq(subject.Features) == 0 {
		return nil
	}
	var ev []domain.Evidence
	for _, candidate := range all {
		if candidate.EntityID == subjectID {
			continue
		}
		if !candidate.Timestamp.Before(subject.Timestamp) {
			continue
		}
		if len(candidate.Features) != len(subject.Features) || featureNormSq(candidate.Features) == 0 {
			continue
		}
		sim := similarity.Cosine(subject.Features, candidate.Features)
		if sim < r.cfg.SimilarityThreshold {
			continue
		}
		ev = append(ev, domain.Evidence{
			Series: candidate.EntityID,
			Time:   candidate.Timestamp,
			Kind:   "similar_state",
			Score:  sim,
			Metrics: map[string]float64{
				"outcome_diff": candidate.Outcome() - subject.Outcome(),
			},
		})
	}
	return ev
}

// buildSignal assembles a Signal from the gathered evidence. Strength is
// the average similarity (intensity of the pattern); Confidence
// additionally factors in sample size, saturating at five samples.
func (r *Recurrence) buildSignal(scopeID uuid.UUID, subject chronos.EntityState, evidence []domain.Evidence) domain.Signal {
	sort.Slice(evidence, func(i, j int) bool { return evidence[i].Score > evidence[j].Score })

	strength := averageScore(evidence)
	confidence := strength * sampleFactor(len(evidence), 5)

	earliest := evidence[0].Time
	for _, e := range evidence {
		if e.Time.Before(earliest) {
			earliest = e.Time
		}
	}

	return domain.Signal{
		ID:              uuid.New(),
		ScopeID:         scopeID,
		Series:          subject.EntityID,
		Pattern:         domain.PatternTypeRecurrence,
		DetectedAt:      r.now(),
		Window:          domain.TimeWindow{Start: earliest, End: subject.Timestamp},
		Strength:        strength,
		Confidence:      confidence,
		ConfidenceClass: ClassifyConfidence(len(evidence), r.cfg.MinSampleSize, r.cfg),
		Evidence:        evidence,
		Explanation:     explainSeries([]chronos.EntityState{subject}, len(evidence), r.cfg.SimilarityThreshold, detectorVersionRecurrence),
		Metrics: map[string]float64{
			"avg_similarity":   strength,
			"sample_size":      float64(len(evidence)),
			"avg_outcome_diff": averageOutcomeDiff(evidence),
		},
	}
}

func averageScore(ev []domain.Evidence) float64 {
	if len(ev) == 0 {
		return 0
	}
	sum := 0.0
	for _, e := range ev {
		sum += e.Score
	}
	return sum / float64(len(ev))
}

func averageOutcomeDiff(ev []domain.Evidence) float64 {
	if len(ev) == 0 {
		return 0
	}
	sum := 0.0
	for _, e := range ev {
		sum += e.Metrics["outcome_diff"]
	}
	return sum / float64(len(ev))
}

// sampleFactor ramps linearly to 1.0 at saturate samples, then plateaus.
// Diminishing returns above the saturation point are intentional: more
// evidence at the same average similarity does not meaningfully sharpen
// the signal.
func sampleFactor(n, saturate int) float64 {
	if saturate <= 0 || n >= saturate {
		return 1.0
	}
	return float64(n) / float64(saturate)
}
