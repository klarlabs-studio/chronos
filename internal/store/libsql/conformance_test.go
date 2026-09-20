package libsql

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos/internal/store/conformance"
)

// TestConformance runs the shared storage contract against libSQL in
// its embedded local-file shape, which needs no network and no Turso
// account. The remote shape runs the same repositories (the provider
// reuses the SQLite implementations over the libSQL driver), so the
// local run covers the repository contract; what it does not cover is
// the remote driver's own wire behaviour.
func TestConformance(t *testing.T) {
	conformance.Run(t, conformance.Backend{
		Name:               "libsql",
		TimestampPrecision: time.Nanosecond,
		New: func(t *testing.T) conformance.Store {
			dsn := "libsql://" + filepath.Join(t.TempDir(), "chronos.db")
			conn, err := openProvider(context.Background(), dsn)
			if err != nil {
				t.Skipf("local libsql open not supported in this environment: %v", err)
			}
			t.Cleanup(func() { _ = conn.Close() })
			return conformance.Store{EntityStates: conn.EntityStates, Signals: conn.Signals}
		},
	})
}
