package domain

import (
	"time"

	"github.com/google/uuid"
)

// perceptionNS is the UUID v5 namespace for content-addressed signal
// IDs. Derived from the URL namespace so it is stable across Chronos
// versions; changing it would rewrite every persisted signal id.
var perceptionNS = uuid.NewSHA1(uuid.NameSpaceURL, []byte("https://github.com/felixgeelhaar/chronos/perception"))

// PerceptionID returns a deterministic UUID v5 for a signal's
// perception identity: (scope, series, pattern, window start/end,
// pairwise partner). Detectors may mint a random UUID; the engine
// overwrites it with this value so a second detect run over the same
// window upserts rather than inserting a duplicate row.
//
// Pairwise patterns (correlation, cross_scope_correlation, divergence,
// convergence) include the partner series from Evidence[0] so two pairs
// that share a window do not collide. Growing window.End produces a
// new id — that is a new perception, not an in-place mutation.
func PerceptionID(s Signal) uuid.UUID {
	partner := uuid.Nil
	switch s.Pattern {
	case PatternTypeCorrelation, PatternTypeCrossScopeCorrelation,
		PatternTypeDivergence, PatternTypeConvergence:
		if len(s.Evidence) > 0 {
			partner = s.Evidence[0].Series
		}
	}
	key := s.ScopeID.String() + "|" +
		s.Series.String() + "|" +
		string(s.Pattern) + "|" +
		s.Window.Start.UTC().Format(time.RFC3339Nano) + "|" +
		s.Window.End.UTC().Format(time.RFC3339Nano) + "|" +
		partner.String()
	return uuid.NewSHA1(perceptionNS, []byte(key))
}
