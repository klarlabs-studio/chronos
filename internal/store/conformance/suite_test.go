package conformance_test

import (
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos/internal/store/conformance"
	"github.com/felixgeelhaar/chronos/internal/store/memory"
)

// TestSuiteAgainstReferenceBackend runs the conformance suite against
// the in-memory store from inside the suite's own package.
//
// The suite is code, and code that is only ever executed from other
// packages' test binaries has nobody to catch it when it breaks: a
// mistake in a fixture or an assertion would surface as five backends
// failing at once with no clue which of the six packages is at fault.
// The in-memory store is the reference implementation of the contract
// (its package doc calls it the canonical backend for tests), so it is
// the natural thing to run the suite against here. Each backend still
// wires the same suite into its own package; that is where a *backend*
// regression shows up.
func TestSuiteAgainstReferenceBackend(t *testing.T) {
	conformance.Run(t, conformance.Backend{
		Name:               "memory (reference)",
		TimestampPrecision: time.Nanosecond,
		New: func(t *testing.T) conformance.Store {
			c := memory.New()
			t.Cleanup(func() { _ = c.Close() })
			return conformance.Store{EntityStates: c.EntityStates, Signals: c.Signals}
		},
	})
}

// TestQuirkDeclaration covers Backend.Has, the switch every pinned
// divergence is routed through.
func TestQuirkDeclaration(t *testing.T) {
	none := conformance.Backend{}
	if none.Has(conformance.QuirkLexicalSubSecondTime) {
		t.Error("a backend declaring no quirks must not report one")
	}
	declared := conformance.Backend{Quirks: []conformance.Quirk{conformance.QuirkLexicalSubSecondTime}}
	if !declared.Has(conformance.QuirkLexicalSubSecondTime) {
		t.Error("a declared quirk must be reported")
	}
	if declared.Has(conformance.Quirk("not-declared")) {
		t.Error("an undeclared quirk must not be reported")
	}
}
