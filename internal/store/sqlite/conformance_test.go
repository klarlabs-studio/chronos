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
		// Timestamps are TEXT with a fixed nine-digit fractional second
		// so lexical ORDER BY matches chronology at nanosecond precision.
		TimestampPrecision: time.Nanosecond,
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
