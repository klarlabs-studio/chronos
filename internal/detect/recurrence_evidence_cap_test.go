package detect

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/google/uuid"
)

// A subject with thousands of similar historical peers must not carry
// thousands of evidence rows: evidence explains the signal, the
// aggregates count it. Production saw ~4,000 rows per recurrence signal,
// 99.9% of all stored evidence.
func TestRecurrence_EvidenceKeepsClosestPeersButAggregatesCountAll(t *testing.T) {
	d := NewRecurrence(defaultCfg())
	scope := uuid.New()
	subject := uuid.New()
	now := time.Now()

	const peers = 3 * maxRecurrenceEvidence
	states := []chronos.EntityState{
		{ID: uuid.New(), EntityID: subject, ScopeID: scope, Timestamp: now, Features: []float64{1, 0}},
	}
	// Peer i sits at angle i*0.001 rad: similarity strictly decreases with i,
	// and the oldest peer is also the least similar one.
	var sumSim float64
	for i := 0; i < peers; i++ {
		angle := float64(i) * 0.001
		sumSim += math.Cos(angle)
		states = append(states, chronos.EntityState{
			ID: uuid.New(), EntityID: uuid.New(), ScopeID: scope,
			Timestamp: now.Add(-time.Duration(i+1) * time.Minute),
			Features:  []float64{math.Cos(angle), math.Sin(angle)},
		})
	}

	got := d.Detect(context.Background(), scope, states)
	var found bool
	for _, s := range got {
		if s.Series != subject {
			continue
		}
		found = true
		if len(s.Evidence) != maxRecurrenceEvidence {
			t.Fatalf("evidence = %d rows, want %d", len(s.Evidence), maxRecurrenceEvidence)
		}
		for i := 1; i < len(s.Evidence); i++ {
			if s.Evidence[i].Score > s.Evidence[i-1].Score {
				t.Fatalf("evidence not ordered by similarity at %d", i)
			}
		}
		if last := s.Evidence[len(s.Evidence)-1].Score; last < math.Cos(float64(maxRecurrenceEvidence-1)*0.001)-1e-12 {
			t.Errorf("kept a less similar peer (%.9f) over a closer one", last)
		}
		if n := s.Metrics["sample_size"]; n != peers {
			t.Errorf("sample_size = %v, want %d (all matches)", n, peers)
		}
		if s.Explanation.ComparablePeers != peers {
			t.Errorf("ComparablePeers = %d, want %d", s.Explanation.ComparablePeers, peers)
		}
		if want := sumSim / peers; math.Abs(s.Strength-want) > 1e-9 {
			t.Errorf("strength = %.9f, want %.9f (mean over all matches)", s.Strength, want)
		}
		if oldest := now.Add(-time.Duration(peers) * time.Minute); !s.Window.Start.Equal(oldest) {
			t.Errorf("window start = %v, want oldest match %v", s.Window.Start, oldest)
		}
		if err := s.Validate(); err != nil {
			t.Errorf("signal invalid: %v", err)
		}
	}
	if !found {
		t.Fatal("no recurrence signal for the subject")
	}
}
