package mysql_test

import (
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos/internal/store/conformance"
)

// TestConformance runs the shared storage contract against a live
// MySQL/MariaDB. It uses the same TEST_MYSQL_DSN gate and the same
// per-test database namespace as the rest of this package's
// integration tests, so CI's integration-mysql job picks it up with no
// new provisioning mechanism.
func TestConformance(t *testing.T) {
	conformance.Run(t, conformance.Backend{
		Name: "mysql",
		// DATETIME(6) stores microseconds; the driver formats
		// time.Time with six fractional digits and truncates the rest.
		TimestampPrecision: time.Microsecond,
		New: func(t *testing.T) conformance.Store {
			conn, _ := withConn(t)
			return conformance.Store{EntityStates: conn.EntityStates, Signals: conn.Signals}
		},
	})
}
