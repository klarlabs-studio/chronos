package chronos_test

import (
	"errors"
	"math"
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos"
	"github.com/google/uuid"
)

// valid returns an EntityState that satisfies every invariant, so each test
// below can violate exactly one thing and attribute the failure to it.
func valid() chronos.EntityState {
	return chronos.EntityState{
		ID:        uuid.New(),
		EntityID:  uuid.New(),
		ScopeID:   uuid.New(),
		Timestamp: time.Date(2026, 9, 20, 12, 0, 0, 0, time.UTC),
		Features:  []float64{1, 2, 3},
		Labels:    []string{"cpu", "mem", "latency"},
	}
}

func TestValidate_AcceptsAWellFormedState(t *testing.T) {
	if err := valid().Validate(); err != nil {
		t.Fatalf("baseline state must validate, got %v", err)
	}
}

// The P0 invariant: non-finite values must not reach detectors. Every
// position is covered because a guard that only checks the first or last
// element is a guard that passes this test by accident.
func TestValidate_RejectsNonFiniteFeatures(t *testing.T) {
	bad := map[string]float64{
		"NaN":  math.NaN(),
		"+Inf": math.Inf(1),
		"-Inf": math.Inf(-1),
	}
	for name, v := range bad {
		for pos := range 3 {
			s := valid()
			s.Features = []float64{1, 2, 3}
			s.Features[pos] = v
			err := s.Validate()
			if !errors.Is(err, chronos.ErrNonFiniteFeature) {
				t.Errorf("%s at index %d: got %v, want ErrNonFiniteFeature", name, pos, err)
			}
		}
	}
}

// Finite extremes are legitimate observations and must survive. Rejecting
// MaxFloat64 would discard real data; the invariant is finiteness, not
// magnitude.
func TestValidate_AcceptsFiniteExtremes(t *testing.T) {
	for name, v := range map[string]float64{
		"max":         math.MaxFloat64,
		"-max":        -math.MaxFloat64,
		"smallest":    math.SmallestNonzeroFloat64,
		"negative":    -1e308,
		"zero":        0,
		"denormal":    5e-324,
		"exactly one": 1,
	} {
		s := valid()
		s.Features = []float64{v, v, v}
		if err := s.Validate(); err != nil {
			t.Errorf("%s (%v): rejected a finite value: %v", name, v, err)
		}
	}
}

func TestValidate_RejectsZeroTimestamp(t *testing.T) {
	s := valid()
	s.Timestamp = time.Time{}
	if err := s.Validate(); !errors.Is(err, chronos.ErrMissingTimestamp) {
		t.Errorf("got %v, want ErrMissingTimestamp", err)
	}
}

func TestValidate_RejectsBlankLabel(t *testing.T) {
	for name, label := range map[string]string{
		"empty":   "",
		"space":   " ",
		"tab":     "\t",
		"newline": "\n",
	} {
		s := valid()
		s.Labels = []string{"cpu", label, "latency"}
		if err := s.Validate(); !errors.Is(err, chronos.ErrEmptyLabel) {
			t.Errorf("%s: got %v, want ErrEmptyLabel", name, err)
		}
	}
}

// Labels stay optional. Tightening blank-label handling must not turn an
// omitted Labels slice into an error.
func TestValidate_LabelsRemainOptional(t *testing.T) {
	s := valid()
	s.Labels = nil
	if err := s.Validate(); err != nil {
		t.Errorf("nil Labels must remain valid, got %v", err)
	}
	s.Labels = []string{}
	if err := s.Validate(); err != nil {
		t.Errorf("empty Labels must remain valid, got %v", err)
	}
}

// Ordering matters for diagnosis: a state that is wrong in several ways
// should report the identity problem first, because that is the one an
// operator can act on without reading the feature vector.
func TestValidate_ReportsIdentityBeforeNumerics(t *testing.T) {
	s := valid()
	s.EntityID = uuid.Nil
	s.Features = []float64{math.NaN()}
	if err := s.Validate(); !errors.Is(err, chronos.ErrMissingEntityID) {
		t.Errorf("got %v, want ErrMissingEntityID to take precedence", err)
	}
}

func TestValidate_RejectsNilObservationID(t *testing.T) {
	s := valid()
	s.ID = uuid.Nil
	if err := s.Validate(); !errors.Is(err, chronos.ErrMissingObservationID) {
		t.Errorf("got %v, want ErrMissingObservationID", err)
	}
}

// A nil observation ID is reported before a missing entity ID: every
// observation must be addressable before we discuss what it observes.
func TestValidate_ReportsObservationIDBeforeEntityID(t *testing.T) {
	s := valid()
	s.ID = uuid.Nil
	s.EntityID = uuid.Nil
	if err := s.Validate(); !errors.Is(err, chronos.ErrMissingObservationID) {
		t.Errorf("got %v, want ErrMissingObservationID to take precedence", err)
	}
}

// The existing invariants must keep working; this suite adds to the
// contract rather than replacing it.
func TestValidate_PreexistingInvariantsHold(t *testing.T) {
	t.Run("missing entity", func(t *testing.T) {
		s := valid()
		s.EntityID = uuid.Nil
		if err := s.Validate(); !errors.Is(err, chronos.ErrMissingEntityID) {
			t.Errorf("got %v", err)
		}
	})
	t.Run("missing scope", func(t *testing.T) {
		s := valid()
		s.ScopeID = uuid.Nil
		if err := s.Validate(); !errors.Is(err, chronos.ErrMissingScopeID) {
			t.Errorf("got %v", err)
		}
	})
	t.Run("no features", func(t *testing.T) {
		s := valid()
		s.Features = nil
		s.Labels = nil
		if err := s.Validate(); !errors.Is(err, chronos.ErrMissingFeatures) {
			t.Errorf("got %v", err)
		}
	})
	t.Run("labels mismatch", func(t *testing.T) {
		s := valid()
		s.Labels = []string{"only-one"}
		if err := s.Validate(); !errors.Is(err, chronos.ErrLabelsMismatch) {
			t.Errorf("got %v", err)
		}
	})
}
