package postgres_test

import (
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos/internal/store/conformance"
)

// TestConformance runs the shared storage contract against a live
// Postgres. It uses the same TEST_POSTGRES_DSN gate and the same
// per-test schema namespace as the rest of this package's integration
// tests, so CI's integration-postgres job picks it up with no new
// provisioning mechanism.
func TestConformance(t *testing.T) {
	conformance.Run(t, conformance.Backend{
		Name: "postgres",
		// TIMESTAMPTZ stores microseconds; pgx truncates the
		// nanosecond remainder on the way in.
		TimestampPrecision: time.Microsecond,
		New: func(t *testing.T) conformance.Store {
			conn := withConn(t)
			return conformance.Store{EntityStates: conn.EntityStates, Signals: conn.Signals}
		},
	})
}
