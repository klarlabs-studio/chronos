// Package sqlite provides a SQLite-backed implementation of the
// persistence ports. The driver is modernc.org/sqlite (pure Go, no CGO)
// so chronos builds and cross-compiles without a C toolchain.
//
// The package owns one [Conn] type that bundles the per-aggregate
// repositories (entity states, signals) on a shared *sql.DB. SQL goes
// through sqlc-generated code in the sqlcgen subpackage; this file is
// only responsible for opening the database, configuring PRAGMAs, and
// applying migrations.
package sqlite

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/felixgeelhaar/chronos/internal/store"
	"github.com/felixgeelhaar/chronos/internal/store/sqlite/sqlcgen"

	// modernc.org/sqlite is a pure-Go SQLite driver; the blank import
	// registers it with database/sql under the name "sqlite". Using a
	// pure-Go driver lets chronos cross-compile without a C toolchain.
	_ "modernc.org/sqlite"
)

func init() {
	store.Register("sqlite", openProvider)
	store.Register("sqlite3", openProvider) // alias the legacy db_type
}

// openProvider is the store.OpenFunc that backs sqlite:// DSNs.
//
// Path resolution from the URL:
//
//   - sqlite://:memory:                 -> ":memory:"  (in-process DB)
//   - sqlite:///abs/path.db             -> "/abs/path.db"
//   - sqlite://relative/path.db         -> "relative/path.db"
//
// Per Mnemos ADR 0001 §3, ?namespace= is accepted on the DSN but
// SQLite has no native schema-namespace concept — operators isolate
// by using a different file. The query parameter is therefore
// validated (so typos are caught) and otherwise ignored.
//
// Bootstrap (Bootstrap exported as a thin alias for [ensureSchema])
// is reused by the libsql provider, since libSQL is wire-compatible
// with SQLite at the SQL level.
func openProvider(_ context.Context, dsn string) (*store.Conn, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite: parse dsn: %w", err)
	}
	if u.Scheme != "sqlite" && u.Scheme != "sqlite3" {
		return nil, fmt.Errorf("sqlite: unsupported scheme %q", u.Scheme)
	}
	if _, err := store.ParseNamespace(u); err != nil {
		return nil, fmt.Errorf("sqlite: %w", err)
	}

	path := pathFromURL(u)
	c, err := Open(path)
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

// txFn returns a closure that satisfies store.Conn.Tx for any
// database/sql-backed provider. The same implementation is reused by
// the libsql provider (libSQL is wire-compatible with SQLite).
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

// TxFn is exported so the libsql provider can reuse this exact
// implementation without duplicating it. Other providers that need
// the same database/sql semantics may also use it.
func TxFn(db *sql.DB) func(ctx context.Context, fn func(context.Context) error) error {
	return txFn(db)
}

// Bootstrap applies the embedded migration to db. Reused by the
// libsql provider since the schema works unchanged on libSQL.
func Bootstrap(db *sql.DB) error {
	return ensureSchema(db)
}

// pathFromURL extracts the SQLite file path from a sqlite:// URL.
// Special cases the in-memory form because url.Parse treats
// ":memory:" as a host, not a path.
func pathFromURL(u *url.URL) string {
	if u.Host == ":memory:" || (u.Host == "" && u.Path == ":memory:") {
		return ":memory:"
	}
	// `sqlite:///abs/path` -> Host="", Path="/abs/path" -> "/abs/path"
	// `sqlite://relative` -> Host="relative", Path="" -> "relative"
	// `sqlite://relative/x` -> Host="relative", Path="/x" -> "relative/x"
	if u.Host == "" {
		return u.Path
	}
	return u.Host + u.Path
}

//go:embed migrations/001_initial.sql
var migrationInitial string

// Conn bundles the SQLite-backed repositories. Construct with [Open].
type Conn struct {
	DB *sql.DB
	q  *sqlcgen.Queries

	EntityStates *EntityStateRepository
	Signals      *SignalRepository
}

// Open connects to a SQLite database at path (or ":memory:") and
// applies migrations. Foreign keys, WAL journal, and a 5-second busy
// timeout are enabled via PRAGMAs encoded in the DSN.
func Open(path string) (*Conn, error) {
	dsn := buildDSN(path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite: open: %w", err)
	}

	// SQLite serialises writes; one connection avoids "database is
	// locked" surprises in tests and small workloads. Production
	// deployments that need higher throughput should use Postgres.
	db.SetMaxOpenConns(1)
	db.SetConnMaxLifetime(0)

	if err := db.PingContext(context.Background()); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite: ping: %w", err)
	}
	if err := ensureSchema(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite: migrate: %w", err)
	}

	q := sqlcgen.New(db)
	c := &Conn{DB: db, q: q}
	c.EntityStates = &EntityStateRepository{conn: c}
	c.Signals = &SignalRepository{conn: c}
	return c, nil
}

// NewFromDB wires the SQLite repositories onto a pre-opened *sql.DB
// without applying PRAGMAs or migrations. The libsql provider uses
// this to reuse the SQLite repositories on a libsql-driver-backed
// connection (libSQL is wire-compatible at the SQL level).
//
// The caller owns db and is responsible for closing it. Conn.Close
// will close it; in shared scenarios call only one of them.
func NewFromDB(db *sql.DB) *Conn {
	q := sqlcgen.New(db)
	c := &Conn{DB: db, q: q}
	c.EntityStates = &EntityStateRepository{conn: c}
	c.Signals = &SignalRepository{conn: c}
	return c
}

// Close releases the underlying database handle.
func (c *Conn) Close() error { return c.DB.Close() }

// buildDSN encodes the engine PRAGMAs as modernc.org/sqlite query
// parameters.
func buildDSN(path string) string {
	q := url.Values{}
	q.Add("_pragma", "foreign_keys(1)")
	q.Add("_pragma", "journal_mode(wal)")
	q.Add("_pragma", "busy_timeout(5000)")
	if path == ":memory:" {
		return "file::memory:?" + q.Encode()
	}
	return "file:" + path + "?" + q.Encode()
}

// ensureSchema applies the embedded migration, then rewrites any
// legacy RFC3339Nano timestamps that trimmed fractional zeros so TEXT
// ORDER BY matches chronology.
func ensureSchema(db *sql.DB) error {
	for _, stmt := range splitStatements(migrationInitial) {
		s := stripLeadingCommentsAndBlanks(stmt)
		if s == "" {
			continue
		}
		if _, err := db.Exec(s); err != nil {
			return fmt.Errorf("apply %q: %w", firstLine(s), err)
		}
	}
	if err := normalizeTimestampText(db); err != nil {
		return fmt.Errorf("normalize timestamps: %w", err)
	}
	return nil
}

// normalizeTimestampText rewrites time columns to the fixed-width
// format produced by [formatTime]. Idempotent: rows already in the
// fixed form are left untouched. Required so databases written before
// the format change keep chronological ORDER BY / range predicates.
func normalizeTimestampText(db *sql.DB) error {
	// Fixed statements only — identifiers are not taken from callers,
	// so gosec G201 does not apply.
	steps := []struct {
		selectSQL string
		updateSQL string
	}{
		{
			`SELECT id, timestamp FROM entity_states`,
			`UPDATE entity_states SET timestamp = ? WHERE id = ?`,
		},
		{
			`SELECT id, detected_at FROM signals`,
			`UPDATE signals SET detected_at = ? WHERE id = ?`,
		},
		{
			`SELECT id, window_start FROM signals`,
			`UPDATE signals SET window_start = ? WHERE id = ?`,
		},
		{
			`SELECT id, window_end FROM signals`,
			`UPDATE signals SET window_end = ? WHERE id = ?`,
		},
		{
			`SELECT rowid, time FROM signal_evidence`,
			`UPDATE signal_evidence SET time = ? WHERE rowid = ?`,
		},
	}
	for _, s := range steps {
		if err := normalizeColumn(db, s.selectSQL, s.updateSQL); err != nil {
			return err
		}
	}
	return nil
}

func normalizeColumn(db *sql.DB, selectSQL, updateSQL string) error {
	rows, err := db.Query(selectSQL)
	if err != nil {
		return fmt.Errorf("normalize select: %w", err)
	}
	defer func() { _ = rows.Close() }()

	type upd struct {
		id   any
		next string
	}
	var updates []upd
	for rows.Next() {
		var id any
		var raw string
		if err := rows.Scan(&id, &raw); err != nil {
			return err
		}
		t, err := parseTime(raw)
		if err != nil || t.IsZero() {
			continue
		}
		next := formatTime(t)
		if next == raw {
			continue
		}
		updates = append(updates, upd{id: id, next: next})
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for _, u := range updates {
		if _, err := db.Exec(updateSQL, u.next, u.id); err != nil {
			return fmt.Errorf("normalize update: %w", err)
		}
	}
	return nil
}

func splitStatements(sqlText string) []string {
	var out []string
	var current strings.Builder
	for _, line := range strings.Split(sqlText, "\n") {
		current.WriteString(line)
		current.WriteString("\n")
		if strings.HasSuffix(strings.TrimSpace(line), ";") {
			out = append(out, current.String())
			current.Reset()
		}
	}
	if rest := strings.TrimSpace(current.String()); rest != "" {
		out = append(out, rest)
	}
	return out
}

// stripLeadingCommentsAndBlanks removes leading `--`-style comment
// lines and blank lines from a candidate statement so the result either
// is a runnable SQL statement or an empty string we skip.
func stripLeadingCommentsAndBlanks(stmt string) string {
	lines := strings.Split(stmt, "\n")
	i := 0
	for ; i < len(lines); i++ {
		t := strings.TrimSpace(lines[i])
		if t == "" || strings.HasPrefix(t, "--") {
			continue
		}
		break
	}
	if i >= len(lines) {
		return ""
	}
	return strings.TrimSpace(strings.Join(lines[i:], "\n"))
}

func firstLine(s string) string {
	if idx := strings.IndexByte(s, '\n'); idx > 0 {
		return s[:idx]
	}
	return s
}

func formatTime(t time.Time) string {
	// Fixed nine fractional digits so TEXT ORDER BY matches chronology.
	// time.RFC3339Nano trims trailing zeros, which makes ".1Z" sort after
	// ".12Z" under byte comparison. See docs/temporal-contract.md and the
	// former QuirkLexicalSubSecondTime.
	return t.UTC().Format("2006-01-02T15:04:05.000000000Z07:00")
}

func parseTime(s string) (time.Time, error) {
	if s == "" {
		return time.Time{}, nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339, s)
}
