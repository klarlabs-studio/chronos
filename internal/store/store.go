// Package store wires Chronos's persistence backends behind a single
// scheme-dispatched factory.
//
// Each provider (memory, sqlite, postgres, libsql, mysql, ...) lives
// in a subpackage and registers itself in init() with [Register] on
// the process-wide [DefaultRegistry]. Consumers blank-import the
// providers they want to support and open connections by DSN through
// [Open]. Isolated [Registry] instances ([NewRegistry], [Registry.Clone])
// let multiple engines coexist without sharing registration state.
// The factory dispatches on the URL scheme so new providers can be
// added without touching this package.
//
// This pattern mirrors Mnemos's ADR 0001 across the cognitive stack
// (Mnemos, Chronos, and sibling tools): the factory contract, URL
// convention, and namespace primitive are shared; the shape of Conn
// is per-tool.
package store

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/felixgeelhaar/chronos/internal/ports"
)

// DefaultNamespace is used when a DSN omits ?namespace=. Single-tenant
// local deployments rarely need to touch this; multi-tool clusters
// (Mnemos + Chronos in one Postgres) set it explicitly per tool.
const DefaultNamespace = "chronos"

// OpenFunc is the provider-side factory signature. Each provider
// registers one of these per scheme it handles.
type OpenFunc func(ctx context.Context, dsn string) (*Conn, error)

// Conn is the engine's port-typed view of an open backend. Every
// provider returns a Conn populated with concrete repositories that
// satisfy the [ports] interfaces.
type Conn struct {
	EntityStates ports.EntityStateRepository
	Signals      ports.SignalRepository

	// Raw is the provider's underlying handle (e.g. *sql.DB for the
	// SQL providers, an in-memory state struct for memory). It is
	// nil-safe to ignore. Tests and provider-specific helpers
	// type-assert against it; the engine itself does not.
	Raw any

	// Closer releases backend resources. Providers populate it; Close
	// invokes it. Repositories must not be used after Close returns.
	Closer func() error

	// Tx, when non-nil, advertises that the provider supports atomic
	// multi-statement units of work and supplies the implementation
	// behind [Conn.InTx]. SQL providers populate it with a BeginTx
	// wrapper; the memory provider leaves it nil because it has no
	// real cross-repository transaction semantics.
	Tx func(ctx context.Context, fn func(context.Context) error) error
}

// ErrNotTransactional is returned by [Conn.InTx] when the underlying
// provider does not support transactions. Callers checking
// [ports.Transactional] via type-assertion will succeed for any Conn
// (Conn always has the InTx method) but should be prepared for this
// error from a provider that opts out — typically the memory backend
// in tests.
var ErrNotTransactional = fmt.Errorf("store: provider does not support transactions")

// Close releases backend resources. Safe to call on a nil Conn or one
// without a Closer set.
func (c *Conn) Close() error {
	if c == nil || c.Closer == nil {
		return nil
	}
	return c.Closer()
}

// InTx satisfies [ports.Transactional] for any Conn whose Tx field
// has been populated by the provider. fn runs inside a provider-
// native transaction; returning a non-nil error rolls back, returning
// nil commits. Repositories that mutate state inside fn do *not* yet
// auto-participate in the transaction — callers wanting strict
// cross-repository atomicity reach through Conn.Raw to issue queries
// against the *sql.Tx provided by Tx. Future work will route
// repository writes through the tx automatically (Mnemos ADR 0001 §4
// describes the same evolution arc).
func (c *Conn) InTx(ctx context.Context, fn func(context.Context) error) error {
	if c == nil || c.Tx == nil {
		return ErrNotTransactional
	}
	return c.Tx(ctx, fn)
}

// Registry holds URL-scheme → OpenFunc bindings. Construct with
// [NewRegistry] (or [Registry.Clone] of [DefaultRegistry]) when a
// process needs isolated provider sets — for example two embed.Engine
// instances that must not share registration state, or a test that
// registers a fake scheme without polluting the process-wide map.
//
// Package-level [Register] / [Open] / [SupportedSchemes] delegate to
// [DefaultRegistry]. Unlike [chronos.Registry] (last-write-wins on
// adapter names), provider registration panics on duplicate schemes so
// collisions surface at startup rather than silently shadowing.
type Registry struct {
	mu       sync.RWMutex
	byScheme map[string]OpenFunc
}

// NewRegistry returns an empty provider registry.
func NewRegistry() *Registry {
	return &Registry{byScheme: make(map[string]OpenFunc)}
}

// Register associates a URL scheme with a provider factory on this
// registry. Panics on empty scheme, nil OpenFunc, or a duplicate
// scheme already present.
func (r *Registry) Register(scheme string, fn OpenFunc) {
	if scheme == "" {
		panic("store: cannot register provider with empty scheme")
	}
	if fn == nil {
		panic("store: cannot register nil OpenFunc for " + scheme)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, dup := r.byScheme[scheme]; dup {
		panic("store: duplicate provider registration for " + scheme)
	}
	r.byScheme[scheme] = fn
}

// Open opens a backend by DSN against this registry. The DSN's URL
// scheme picks the provider; everything after the scheme is
// provider-specific.
//
// Examples:
//
//	memory://?namespace=chronos
//	sqlite:///var/lib/chronos/chronos.db
//	postgres://user:pw@host:5432/cogstack?namespace=chronos
//	mysql://user:pw@host:3306/?namespace=chronos
//	libsql://my-db.turso.io?authToken=...
func (r *Registry) Open(ctx context.Context, dsn string) (*Conn, error) {
	scheme, _, ok := strings.Cut(dsn, "://")
	if !ok || scheme == "" {
		return nil, fmt.Errorf("store: dsn %q missing scheme://", dsn)
	}
	r.mu.RLock()
	fn, found := r.byScheme[scheme]
	r.mu.RUnlock()
	if !found {
		return nil, fmt.Errorf("store: unknown provider %q (registered: %v)",
			scheme, r.Schemes())
	}
	return fn(ctx, dsn)
}

// Schemes returns the registered scheme names in sorted order.
func (r *Registry) Schemes() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.byScheme))
	for k := range r.byScheme {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// Clone returns a new Registry whose scheme map is a snapshot of r.
// OpenFuncs are copied by reference (they are typically package-level
// functions). Subsequent Register calls on the clone or the original
// do not affect each other.
func (r *Registry) Clone() *Registry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := NewRegistry()
	for k, fn := range r.byScheme {
		out.byScheme[k] = fn
	}
	return out
}

// defaultRegistry is the process-wide convenience registry. Providers
// register into it from init(); prefer [NewRegistry] or
// [Registry.Clone] when embedding multiple independently configured
// Chronos engines that need isolated provider sets.
var defaultRegistry = NewRegistry()

// DefaultRegistry returns the process-wide provider registry used by
// [Register], [Open], and [SupportedSchemes].
func DefaultRegistry() *Registry { return defaultRegistry }

// Register associates a URL scheme with a provider factory on the
// default registry. Providers invoke this from init(). See
// [Registry.Register].
func Register(scheme string, fn OpenFunc) { defaultRegistry.Register(scheme, fn) }

// Open opens a backend by DSN against the default registry. The
// matching provider must have been blank-imported by the binary
// (e.g. _ "github.com/felixgeelhaar/chronos/internal/store/sqlite") so
// that its init() runs before Open is called. See [Registry.Open].
func Open(ctx context.Context, dsn string) (*Conn, error) {
	return defaultRegistry.Open(ctx, dsn)
}

// SupportedSchemes returns the schemes registered on the default
// registry in sorted order. Useful for error messages and CLI help.
func SupportedSchemes() []string { return defaultRegistry.Schemes() }

// namespaceRE mirrors the Mnemos ADR 0001 namespace contract:
// lowercase alphanumeric+underscore, must start with a letter,
// max 63 bytes (Postgres identifier limit and a sensible ceiling
// for every other dialect).
var namespaceRE = regexp.MustCompile(`^[a-z][a-z0-9_]{0,62}$`)

// ParseNamespace extracts the namespace from a parsed URL's query
// string, applying [DefaultNamespace] when absent and validating
// against [namespaceRE]. Providers call this when they need to
// translate the namespace into a native isolation primitive.
func ParseNamespace(u *url.URL) (string, error) {
	ns := u.Query().Get("namespace")
	if ns == "" {
		return DefaultNamespace, nil
	}
	if !namespaceRE.MatchString(ns) {
		return "", fmt.Errorf("store: invalid namespace %q (want %s)", ns, namespaceRE.String())
	}
	return ns, nil
}

// LegacyDSN converts the deprecated CHRONOS_DB_TYPE + CHRONOS_DB_CONN
// pair into the new DSN form. It exists to keep existing operator
// configurations working through the cutover; new deployments should
// set CHRONOS_DB_DSN directly.
//
// The translation:
//
//	memory                          -> memory://
//	sqlite / sqlite3, path          -> sqlite:///<path>   (or sqlite://:memory: for ":memory:")
//	postgres / postgresql, libpq    -> postgres://...     (libpq URL is already a DSN; pass through)
//
// If dbType is empty, the empty DSN is returned and the caller should
// fall back to its own default.
func LegacyDSN(dbType, conn string) (string, error) {
	switch dbType {
	case "":
		return "", nil
	case "memory":
		return "memory://", nil
	case "sqlite", "sqlite3":
		if conn == ":memory:" {
			return "sqlite://:memory:", nil
		}
		// Always emit an absolute-style path (sqlite:///path). The
		// SQLite provider strips the leading slash on relative paths
		// so "sqlite:///chronos.db" still opens ./chronos.db when
		// the working directory is the project root.
		return "sqlite://" + conn, nil
	case "postgres", "postgresql":
		// libpq DSNs already encode the scheme. If the operator
		// supplied a `postgres://...` URL, return it unchanged; if
		// they supplied a key-value libpq form, prefix it explicitly.
		if strings.HasPrefix(conn, "postgres://") || strings.HasPrefix(conn, "postgresql://") {
			return conn, nil
		}
		return "", fmt.Errorf("store: legacy postgres conn must be a postgres:// URL, not key=value form: %q", conn)
	default:
		return "", fmt.Errorf("store: unknown legacy db_type %q", dbType)
	}
}
