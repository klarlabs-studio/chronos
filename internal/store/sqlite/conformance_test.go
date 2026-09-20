package sqlite

import (
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos/internal/store/conformance"
)

// TestConformance runs the shared storage contract against SQLite.
func TestConformance(t *testing.T) {
	conformance.Run(t, conformance.Backend{
		Name: "sqlite",
		// Timestamps are TEXT in RFC3339Nano, so the full nanosecond
		// is stored and read back.
		TimestampPrecision: time.Nanosecond,
		// ...but ordering and range predicates compare those strings,
		// and RFC3339Nano trims trailing zeros, so byte order stops
		// matching chronological order below one second.
		Quirks: []conformance.Quirk{conformance.QuirkLexicalSubSecondTime},
		New: func(t *testing.T) conformance.Store {
			c, err := Open(":memory:")
			if err != nil {
				t.Fatalf("sqlite.Open(:memory:): %v", err)
			}
			t.Cleanup(func() { _ = c.Close() })
			return conformance.Store{EntityStates: c.EntityStates, Signals: c.Signals}
		},
	})
}
