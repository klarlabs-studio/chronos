package store_test

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"testing"

	"github.com/felixgeelhaar/chronos/internal/ports"
	"github.com/felixgeelhaar/chronos/internal/store"

	// Pull in the providers we want to exercise. Memory and sqlite
	// are sufficient to cover registry dispatch + namespace handling
	// without needing a live Postgres or MySQL.
	_ "github.com/felixgeelhaar/chronos/internal/store/memory"
	_ "github.com/felixgeelhaar/chronos/internal/store/sqlite"
)

func TestOpen_RoutesByScheme(t *testing.T) {
	cases := []struct {
		name string
		dsn  string
	}{
		{"memory", "memory://?namespace=chronos"},
		{"sqlite in-memory", "sqlite://:memory:"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			conn, err := store.Open(context.Background(), tc.dsn)
			if err != nil {
				t.Fatalf("Open(%q): %v", tc.dsn, err)
			}
			defer func() { _ = conn.Close() }()
			if conn.EntityStates == nil || conn.Signals == nil {
				t.Errorf("Conn has nil repositories: %+v", conn)
			}
		})
	}
}

func TestOpen_RejectsMissingScheme(t *testing.T) {
	_, err := store.Open(context.Background(), "no-scheme-here")
	if err == nil {
		t.Fatal("expected error on schemeless dsn")
	}
	if !strings.Contains(err.Error(), "missing scheme") {
		t.Errorf("error = %q", err.Error())
	}
}

func TestOpen_RejectsUnknownScheme(t *testing.T) {
	_, err := store.Open(context.Background(), "neverheard://anywhere")
	if err == nil {
		t.Fatal("expected error on unknown scheme")
	}
	if !strings.Contains(err.Error(), "unknown provider") {
		t.Errorf("error = %q", err.Error())
	}
}

func TestSupportedSchemes_IncludesRegisteredProviders(t *testing.T) {
	got := store.SupportedSchemes()
	for _, want := range []string{"memory", "sqlite"} {
		found := false
		for _, s := range got {
			if s == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("SupportedSchemes() missing %q; got %v", want, got)
		}
	}
}

func TestParseNamespace(t *testing.T) {
	cases := []struct {
		name    string
		query   string
		want    string
		wantErr bool
	}{
		{"empty falls back to default", "", store.DefaultNamespace, false},
		{"explicit valid", "namespace=tenant_a", "tenant_a", false},
		{"upper-case rejected", "namespace=Tenant", "", true},
		{"leading digit rejected", "namespace=1tenant", "", true},
		{"hyphen rejected", "namespace=tenant-a", "", true},
		{"too long rejected", "namespace=" + strings.Repeat("a", 64), "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			u := &url.URL{RawQuery: tc.query}
			got, err := store.ParseNamespace(u)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ParseNamespace err = %v, wantErr=%v", err, tc.wantErr)
			}
			if !tc.wantErr && got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestLegacyDSN(t *testing.T) {
	cases := []struct {
		name    string
		dbType  string
		conn    string
		want    string
		wantErr bool
	}{
		{"empty type returns empty", "", "anything", "", false},
		{"memory", "memory", "ignored", "memory://", false},
		{"sqlite path", "sqlite", "chronos.db", "sqlite://chronos.db", false},
		{"sqlite alias", "sqlite3", "chronos.db", "sqlite://chronos.db", false},
		{"sqlite in-memory", "sqlite", ":memory:", "sqlite://:memory:", false},
		{"postgres url passthrough", "postgres", "postgres://u:p@h/db", "postgres://u:p@h/db", false},
		{"postgresql url passthrough", "postgresql", "postgresql://u@h/db", "postgresql://u@h/db", false},
		{"libpq kv form rejected", "postgres", "host=localhost user=foo", "", true},
		{"unknown type rejected", "duckdb", "x", "", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := store.LegacyDSN(tc.dbType, tc.conn)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr=%v", err, tc.wantErr)
			}
			if !tc.wantErr && got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRegister_PanicsOnDuplicate(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on duplicate registration")
		}
	}()
	store.Register("memory", func(context.Context, string) (*store.Conn, error) { return nil, nil })
}

func TestRegister_PanicsOnEmptyScheme(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on empty scheme")
		}
	}()
	store.Register("", func(context.Context, string) (*store.Conn, error) { return nil, nil })
}

// TestTransactional_AssertableOnSQLProvider demonstrates the
// capability-negotiation pattern: the engine type-asserts
// ports.Transactional and falls back when the provider opts out.
// Today the SQL providers populate Tx, the memory provider does not.
func TestTransactional_AssertableOnSQLProvider(t *testing.T) {
	conn, err := store.Open(context.Background(), "sqlite://:memory:")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer func() { _ = conn.Close() }()

	// Type-assert against the optional capability interface.
	tx, ok := any(conn).(ports.Transactional)
	if !ok {
		t.Fatal("Conn should satisfy ports.Transactional via its InTx method")
	}

	calls := 0
	if err := tx.InTx(context.Background(), func(_ context.Context) error {
		calls++
		return nil
	}); err != nil {
		t.Fatalf("InTx: %v", err)
	}
	if calls != 1 {
		t.Errorf("fn calls = %d, want 1", calls)
	}
}

func TestTransactional_RollsBackOnError(t *testing.T) {
	conn, err := store.Open(context.Background(), "sqlite://:memory:")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer func() { _ = conn.Close() }()

	want := errors.New("intentional rollback")
	got := conn.InTx(context.Background(), func(_ context.Context) error {
		return want
	})
	if !errors.Is(got, want) {
		t.Errorf("InTx error = %v, want %v", got, want)
	}
}

func TestTransactional_MemoryReturnsNotTransactional(t *testing.T) {
	// The memory provider deliberately doesn't populate Conn.Tx;
	// callers should fall back rather than crash.
	conn, err := store.Open(context.Background(), "memory://")
	if err != nil {
		t.Fatalf("open memory: %v", err)
	}
	defer func() { _ = conn.Close() }()

	err = conn.InTx(context.Background(), func(context.Context) error { return nil })
	if !errors.Is(err, store.ErrNotTransactional) {
		t.Errorf("InTx on memory = %v, want ErrNotTransactional", err)
	}
}

func TestRegister_PanicsOnNilFunc(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on nil OpenFunc")
		}
	}()
	store.Register("nilfn", nil)
}

func TestRegistry_CloneIsolatesRegistration(t *testing.T) {
	base := store.DefaultRegistry().Clone()
	isolated := base.Clone()

	opened := false
	isolated.Register("fakestore", func(context.Context, string) (*store.Conn, error) {
		opened = true
		return &store.Conn{}, nil
	})

	// Clone must not see schemes registered only on the sibling.
	if _, err := base.Open(context.Background(), "fakestore://"); err == nil {
		t.Fatal("base registry must not resolve scheme registered only on clone")
	}
	conn, err := isolated.Open(context.Background(), "fakestore://x")
	if err != nil {
		t.Fatalf("isolated.Open: %v", err)
	}
	_ = conn.Close()
	if !opened {
		t.Fatal("expected fake OpenFunc to run on isolated registry")
	}

	// Default registry must remain untouched.
	for _, s := range store.SupportedSchemes() {
		if s == "fakestore" {
			t.Fatal("default registry must not gain schemes registered on a clone")
		}
	}
}

func TestRegistry_OpenRejectsUnknownScheme(t *testing.T) {
	r := store.NewRegistry()
	_, err := r.Open(context.Background(), "memory://")
	if err == nil {
		t.Fatal("empty registry must reject memory://")
	}
	if !strings.Contains(err.Error(), "unknown provider") {
		t.Errorf("error = %q", err.Error())
	}
}

func TestRegistry_CloneCopiesExistingSchemes(t *testing.T) {
	r := store.DefaultRegistry().Clone()
	conn, err := r.Open(context.Background(), "memory://?namespace=clone_test")
	if err != nil {
		t.Fatalf("Clone().Open(memory): %v", err)
	}
	defer func() { _ = conn.Close() }()
	if conn.EntityStates == nil {
		t.Fatal("expected repositories on cloned-registry Conn")
	}
}
