package memory

import (
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos/internal/store/conformance"
)

// TestConformance runs the shared storage contract against the
// in-memory backend. The suite is identical for every backend; only
// the declared precision and quirks differ.
func TestConformance(t *testing.T) {
	conformance.Run(t, conformance.Backend{
		Name: "memory",
		// Timestamps are kept as time.Time, so nothing is lost.
		TimestampPrecision: time.Nanosecond,
		New: func(t *testing.T) conformance.Store {
			c := New()
			t.Cleanup(func() { _ = c.Close() })
			return conformance.Store{EntityStates: c.EntityStates, Signals: c.Signals}
		},
	})
}
