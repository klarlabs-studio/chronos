// Package postgres provides a PostgreSQL-backed implementation of the
// persistence ports. Postgres is the recommended backend for multi-
// process deployments; SQLite covers single-binary, embedded, and test
// use cases.
//
// Wire-protocol compatibles supported by this provider via the same
// driver: CockroachDB, Yugabyte, Neon, Crunchy Bridge, TimescaleDB,
// AlloyDB Omni. They all speak the libpq wire protocol; Chronos
// doesn't use any postgres-specific extension.
//
// Per Mnemos ADR 0001 §3, ?namespace= translates to a Postgres
// schema: CREATE SCHEMA IF NOT EXISTS <ns> + SET search_path TO <ns>
// runs at every Open. The schema-version table lives inside the
// namespace, so two tools (Mnemos and Chronos) sharing one Postgres
// database track their migrations independently.
package postgres

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/felixgeelhaar/chronos/internal/store"

	// pgx-stdlib registers the "pgx" sql.DB driver in init(). Replaces
	// lib/pq so the cognitive stack (Mnemos, Chronos, and sibling tools)
	// shares one driver surface.
	_ "github.com/jackc/pgx/v5/stdlib"
)

//go:embed migrations/001_initial.sql
var migrationInitial string

// connTimeout caps the lifetime of pooled connections.
const connTimeout = 5 * time.Minute

// Conn bundles the Postgres-backed repositories.
type Conn struct {
	DB *sql.DB

	EntityStates *EntityStateRepository
	Signals      *SignalRepository
}

func init() {
	store.Register("postgres", openProvider)
	store.Register("postgresql", openProvider)
}

// openProvider is the store.OpenFunc that backs postgres:// /
// postgresql:// DSNs. It strips the ?namespace= query parameter
// before handing the DSN to pgx (pgx rejects unknown query keys),
// then ensures the schema exists and sets search_path so all
// subsequent queries land in the namespace.
func openProvider(ctx context.Context, dsn string) (*store.Conn, error) {
	parsed, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	c, err := openWithNamespace(ctx, parsed)
	if err != nil {
		return nil, err
	}
	return &store.Conn{
		EntityStates: c.EntityStates,
		Signals:      c.Signals,
		Raw:          c.DB,
		Closer:       c.Close,
		Tx:           txFn(c.DB),
	}, nil
}

func txFn(db *sql.DB) func(ctx context.Context, fn func(context.Context) error) error {
	return func(ctx context.Context, fn func(context.Context) error) (err error) {
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		defer func() {
			if p := recover(); p != nil {
				_ = tx.Rollback()
				panic(p)
			}
			if err != nil {
				_ = tx.Rollback()
			}
		}()
		if err = fn(ctx); err != nil {
			return err
		}
		return tx.Commit()
	}
}

// dsnParts holds the relevant pieces of a parsed Chronos postgres DSN.
type dsnParts struct {
	// Driver is the libpq-shaped DSN with ?namespace= stripped, ready
	// to hand to pgx.
	Driver string
	// Namespace is the validated schema name (default "chronos").
	Namespace string
}

func parseDSN(dsn string) (dsnParts, error) {
	if !strings.HasPrefix(dsn, "postgres://") && !strings.HasPrefix(dsn, "postgresql://") {
		return dsnParts{}, fmt.Errorf("postgres: not a postgres dsn: %q", dsn)
	}
	u, err := url.Parse(dsn)
	if err != nil {
		return dsnParts{}, fmt.Errorf("postgres: parse dsn: %w", err)
	}
	ns, err := store.ParseNamespace(u)
	if err != nil {
		return dsnParts{}, err
	}
	q := u.Query()
	q.Del("namespace")
	// search_path travels in the DSN, not in a SET statement, because
	// SET is a property of one session and *sql.DB is a pool of up to
	// 25 of them. A SET issued at Open reaches whichever connection
	// happened to serve it; every connection the pool opens later
	// starts on the default search_path, where the namespace's tables
	// are not visible, and the query fails with
	// `relation "entity_states" does not exist`. Single-threaded use
	// hides this because the pool keeps handing back the one warm
	// connection. Passing search_path as a startup runtime parameter
	// puts it on every connection the pool will ever open.
	q.Set("search_path", ns)
	u.RawQuery = q.Encode()
	return dsnParts{Driver: u.String(), Namespace: ns}, nil
}

// Open is kept exported for tests and back-compat callers; the
// preferred entry point is store.Open(ctx, "postgres://...").
func Open(connStr string) (*Conn, error) {
	parsed, err := parseDSN(connStr)
	if err != nil {
		// Allow back-compat callers to pass a libpq URL without
		// namespace; in that case parseDSN returns an error only on
		// truly malformed input.
		return nil, err
	}
	return openWithNamespace(context.Background(), parsed)
}

func openWithNamespace(ctx context.Context, p dsnParts) (*Conn, error) {
	db, err := sql.Open("pgx", p.Driver)
	if err != nil {
		return nil, fmt.Errorf("postgres: open: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(connTimeout)

	if err := applyNamespace(ctx, db, p.Namespace); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := ensureSchema(ctx, db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("postgres: migrate: %w", err)
	}

	c := &Conn{DB: db}
	c.EntityStates = &EntityStateRepository{conn: c}
	c.Signals = &SignalRepository{conn: c}
	return c, nil
}

// applyNamespace runs CREATE SCHEMA IF NOT EXISTS. The schema name has
// already been validated against namespaceRE so fmt-substitution is
// safe — Postgres identifiers are not parameter-substitutable in
// CREATE SCHEMA, so we cannot use a placeholder.
//
// search_path is not set here: it is carried on every connection as a
// startup parameter by parseDSN. A schema named in search_path need
// not exist yet, so connecting before this runs is fine.
func applyNamespace(ctx context.Context, db *sql.DB, ns string) error {
	if _, err := db.ExecContext(ctx, fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s", ns)); err != nil {
		return fmt.Errorf("postgres: create schema %q: %w", ns, err)
	}
	return nil
}

// Close releases the underlying database handle.
func (c *Conn) Close() error { return c.DB.Close() }

func ensureSchema(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, migrationInitial); err != nil {
		return fmt.Errorf("apply migration: %w", err)
	}
	return nil
}
