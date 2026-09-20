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

// Anomaly detects PatternTypeAnomaly: an entity whose most recent
// state is unlike the most recent states of its peers in the same
// scope. It is the cross-entity dual of Recurrence — Recurrence
// asks "have peers been here before?", Anomaly asks "are peers
// nowhere near where this entity is right now?"
//
// Method: cosine similarity between the subject's current state and
// every other entity's current state. If the *highest* similarity
// across peers is below CHRONOS_ANOMALY_MAX_SIM, the subject is
// isolated → emit. Strength is 1 - max similarity (more isolated =
// stronger anomaly); Confidence scales by peer-count saturation.
//
// Evidence is a "peer_distance" record per peer, sorted by similarity
// descending so the closest peer (still below threshold) appears first.
type Anomaly struct {
	cfg *config.Config
	now func() time.Time
}

// NewAnomaly wires an Anomaly detector from configuration.
func NewAnomaly(cfg *config.Config) *Anomaly { return &Anomaly{cfg: cfg, now: time.Now} }

// Pattern reports the PatternType this detector emits.
func (a *Anomaly) Pattern() domain.PatternType { return domain.PatternTypeAnomaly }

// Detect compares each entity's most recent state against the most
// recent states of other entities in the same scope.
func (a *Anomaly) Detect(_ context.Context, scopeID uuid.UUID, states []chronos.EntityState) []domain.Signal {
	if a.cfg.AnomalyMinPeers < 1 {
		return nil
	}
	latest := mostRecentByEntity(states)
	if len(latest) < 2 {
		return nil
	}

	var signals []domain.Signal
	for _, entityID := range sortedUUIDKeys(latest) {
		subject := latest[entityID]
		evidence := a.peerEvidence(entityID, subject, latest)
		if len(evidence) < a.cfg.AnomalyMinPeers {
			continue
		}
		// Sort highest-similarity-first so we can read max from
		// evidence[0].Score.
		sort.Slice(evidence, func(i, j int) bool { return evidence[i].Score > evidence[j].Score })
		maxSim := evidence[0].Score
		if maxSim >= a.cfg.AnomalyMaxSimilarity {
			continue
		}
		signals = append(signals, a.build(scopeID, entityID, subject, evidence, maxSim))
	}
	return keepValid(signals)
}

func (a *Anomaly) peerEvidence(subjectID uuid.UUID, subject chronos.EntityState, latest map[uuid.UUID]chronos.EntityState) []domain.Evidence {
	// A zero-norm feature vector has no direction. Cosine reports 0 for
	// it, which would read as "maximally isolated" if we treated that as
	// a real distance. Skip the subject entirely — no comparable state.
	if featureNormSq(subject.Features) == 0 {
		return nil
	}
	var ev []domain.Evidence
	for _, peerID := range sortedUUIDKeys(latest) {
		if peerID == subjectID {
			continue
		}
		peer := latest[peerID]
		if len(peer.Features) != len(subject.Features) || featureNormSq(peer.Features) == 0 {
			continue
		}
		ev = append(ev, domain.Evidence{
			Series: peerID,
			Time:   peer.Timestamp,
			Kind:   "peer_distance",
			Score:  similarity.Cosine(subject.Features, peer.Features),
		})
	}
	return ev
}

func (a *Anomaly) build(scopeID, subjectID uuid.UUID, subject chronos.EntityState, evidence []domain.Evidence, maxSim float64) domain.Signal {
	strength := clamp01(1 - maxSim)
	confidence := strength * sampleFactor(len(evidence), 5)
	return domain.Signal{
		ID:         uuid.New(),
		ScopeID:    scopeID,
		Series:     subjectID,
		Pattern:    domain.PatternTypeAnomaly,
		DetectedAt: a.now(),
		// Anomaly is a snapshot in time across peers; the window is
		// degenerate (start == end at the subject's observation).
		Window:          domain.TimeWindow{Start: subject.Timestamp, End: subject.Timestamp},
		Strength:        strength,
		Confidence:      confidence,
		ConfidenceClass: ClassifyConfidence(len(evidence), a.cfg.AnomalyMinPeers, a.cfg),
		Evidence:        evidence,
		Explanation:     explainSeries([]chronos.EntityState{subject}, len(evidence), a.cfg.AnomalyMaxSimilarity, detectorVersionAnomaly),
		Metrics: map[string]float64{
			"max_peer_similarity": maxSim,
			"peer_count":          float64(len(evidence)),
		},
	}
}
