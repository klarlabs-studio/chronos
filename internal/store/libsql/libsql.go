// Package libsql implements a [store] provider backed by libSQL —
// the SQLite-compatible engine behind Turso. The wire SQL dialect
// matches SQLite, so this provider reuses the SQLite schema and
// repository implementations: only the registration, DSN passthrough,
// and database/sql driver name change.
//
// Two deployment shapes are supported:
//
//  1. Remote (Turso, custom libSQL servers):
//     libsql://my-db.turso.io?authToken=eyJ...
//
//  2. Local file (libSQL embedded; behaves like SQLite for compatibility
//     tests and offline-first deployments):
//     libsql:///absolute/path/to/local.db
//
// The pure-Go libsql-client-go driver keeps CGO_ENABLED=0 — Chronos's
// pinned default. Operators who want an embedded libSQL build with
// CGO can blank-import a different driver in their fork; this provider
// does not depend on CGO.
//
// Per Mnemos ADR 0001 §3, every provider exposes a `?namespace=`
// query param. libSQL has no per-tenant schema concept and each remote
// database already represents a tenant boundary, so namespace is
// validated (so typos surface) and otherwise ignored — matching
// Mnemos's libsql posture for the same reason.
package libsql

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"strings"

	"github.com/felixgeelhaar/chronos/internal/store"
	"github.com/felixgeelhaar/chronos/internal/store/sqlite"

	// libsql-client-go registers the "libsql" sql.DB driver in init().
	_ "github.com/tursodatabase/libsql-client-go/libsql"
)

// driverName is the database/sql driver name registered by
// libsql-client-go.
const driverName = "libsql"

func init() {
	store.Register("libsql", openProvider)
}

// openProvider parses a libsql:// URL, opens the database via the
// libSQL driver, applies the SQLite schema (libSQL is wire-compatible),
// and returns a Conn populated with the existing SQLite repository
// implementations.
func openProvider(ctx context.Context, dsn string) (*store.Conn, error) {
	driverDSN, err := translateDSN(dsn)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open(driverName, driverDSN)
	if err != nil {
		return nil, fmt.Errorf("libsql: sql.Open: %w", err)
	}
	if strings.HasPrefix(driverDSN, "file:") {
		// Embedded local-file mode is SQLite on disk, and SQLite
		// serialises writes: a second connection writing concurrently
		// gets SQLITE_BUSY and the write is lost, not retried. The
		// SQLite provider caps the pool at one connection for exactly
		// this reason; the libSQL provider reuses that provider's
		// repositories, so it has to reuse the constraint they were
		// written under. Remote databases keep the default pool —
		// there the server, not the file lock, arbitrates writes.
		db.SetMaxOpenConns(1)
		db.SetConnMaxLifetime(0)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("libsql: ping: %w (dsn=%s)", err, redactDSN(driverDSN))
	}
	if err := sqlite.Bootstrap(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("libsql: bootstrap schema: %w", err)
	}

	c := sqlite.NewFromDB(db)
	return &store.Conn{
		EntityStates: c.EntityStates,
		Signals:      c.Signals,
		Raw:          db,
		Closer:       c.Close,
		Tx:           sqlite.TxFn(db),
	}, nil
}

// translateDSN converts a Chronos libsql:// URL into the form the
// libsql-client-go driver expects.
//
//   - libsql://my-db.turso.io?authToken=xxx → passes through (the
//     driver speaks this URL form natively).
//   - libsql:///absolute/path.db → "file:/absolute/path.db" so the
//     driver opens an embedded local file.
//
// The `?namespace=` query param is silently dropped if present —
// libSQL has no schema-namespace concept. Validating it here means
// invalid namespaces still error early.
func translateDSN(dsn string) (string, error) {
	if !strings.HasPrefix(dsn, "libsql://") {
		return "", fmt.Errorf("libsql: not a libsql dsn: %q", dsn)
	}
	u, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("libsql: parse dsn: %w", err)
	}
	if _, err := store.ParseNamespace(u); err != nil {
		return "", fmt.Errorf("libsql: %w", err)
	}
	q := u.Query()
	q.Del("namespace")
	u.RawQuery = q.Encode()

	// Remote shape: host is set (e.g. "my-db.turso.io"). Pass
	// through unchanged.
	if u.Host != "" {
		return u.String(), nil
	}
	// Local-file shape: libsql:///abs/path → file:/abs/path.
	if u.Path == "" {
		return "", fmt.Errorf("libsql: dsn missing host or path: %q", dsn)
	}
	return "file:" + u.Path, nil
}

// redactDSN strips obvious credential parameters before logging. It is
// intentionally simple — operators should treat any DSN-bearing log
// line as sensitive even when redacted.
func redactDSN(dsn string) string {
	u, err := url.Parse(dsn)
	if err != nil {
		return dsn
	}
	q := u.Query()
	for _, k := range []string{"authToken", "auth_token", "password"} {
		if q.Has(k) {
			q.Set(k, "REDACTED")
		}
	}
	u.RawQuery = q.Encode()
	return u.String()
}
