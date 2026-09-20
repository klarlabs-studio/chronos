package postgres

import (
	"math"
	"net/url"
	"strings"
	"testing"
)

// Unit tests for the DSN parser. The integration suite that exercises
// a live Postgres server lives in integration_test.go and is gated on
// TEST_POSTGRES_DSN.

func TestParseDSN(t *testing.T) {
	cases := []struct {
		name           string
		in             string
		wantNS         string
		wantNoQueryKey string // namespace must NOT appear in the driver DSN
		wantErr        bool
	}{
		{
			name:           "default namespace",
			in:             "postgres://user:pw@host:5432/db",
			wantNS:         "chronos",
			wantNoQueryKey: "namespace",
		},
		{
			name:           "explicit namespace stripped from driver DSN",
			in:             "postgres://user:pw@host/db?namespace=tenant_a",
			wantNS:         "tenant_a",
			wantNoQueryKey: "namespace",
		},
		{
			name:           "postgresql alias accepted",
			in:             "postgresql://user@host/db?namespace=chronos",
			wantNS:         "chronos",
			wantNoQueryKey: "namespace",
		},
		{
			name:    "wrong scheme rejected",
			in:      "mysql://h/db",
			wantErr: true,
		},
		{
			name:    "invalid namespace rejected",
			in:      "postgres://h/db?namespace=Bad-Name",
			wantErr: true,
		},
		{
			name:    "uppercase namespace rejected",
			in:      "postgres://h/db?namespace=Tenant",
			wantErr: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parseDSN(tc.in)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr=%v", err, tc.wantErr)
			}
			if tc.wantErr {
				return
			}
			if got.Namespace != tc.wantNS {
				t.Errorf("Namespace = %q, want %q", got.Namespace, tc.wantNS)
			}
			if tc.wantNoQueryKey != "" && strings.Contains(got.Driver, tc.wantNoQueryKey+"=") {
				t.Errorf("driver DSN should not contain %q=:\n  got: %s", tc.wantNoQueryKey, got.Driver)
			}
			// Driver DSN must still start with the original scheme so
			// pgx routes it correctly.
			if !strings.HasPrefix(got.Driver, "postgres://") && !strings.HasPrefix(got.Driver, "postgresql://") {
				t.Errorf("driver DSN should preserve scheme: %s", got.Driver)
			}
		})
	}
}

// TestParseDSN_CarriesNamespaceAsSearchPath pins the fix for a bug the
// storage conformance suite caught: the namespace has to reach every
// connection the pool opens, not just whichever one served Open. A
// `SET search_path` statement is per-session, so the second pooled
// connection saw the default search_path and every query on it failed
// with `relation "entity_states" does not exist`. Carrying it as a
// startup runtime parameter in the DSN puts it on all of them.
func TestParseDSN_CarriesNamespaceAsSearchPath(t *testing.T) {
	cases := []struct{ in, want string }{
		{"postgres://h/db", "chronos"},
		{"postgres://h/db?namespace=tenant_a", "tenant_a"},
		// No credentials in this fixture on purpose: the security scan
		// flags a user:password pair in a DSN literal, and this case is
		// about the query string, not about auth.
		{"postgresql://h:5432/db?sslmode=require&namespace=tenant_b", "tenant_b"},
	}
	for _, tc := range cases {
		got, err := parseDSN(tc.in)
		if err != nil {
			t.Fatalf("parseDSN(%q): %v", tc.in, err)
		}
		u, err := url.Parse(got.Driver)
		if err != nil {
			t.Fatalf("driver dsn %q is not a URL: %v", got.Driver, err)
		}
		if sp := u.Query().Get("search_path"); sp != tc.want {
			t.Errorf("parseDSN(%q) driver search_path = %q, want %q (dsn: %s)",
				tc.in, sp, tc.want, got.Driver)
		}
	}
}

func TestParseDSN_PreservesNonNamespaceQueryKeys(t *testing.T) {
	in := "postgres://h/db?sslmode=require&namespace=foo&application_name=chronos"
	got, err := parseDSN(in)
	if err != nil {
		t.Fatalf("parseDSN: %v", err)
	}
	if got.Namespace != "foo" {
		t.Errorf("namespace = %q", got.Namespace)
	}
	for _, want := range []string{"sslmode=require", "application_name=chronos"} {
		if !strings.Contains(got.Driver, want) {
			t.Errorf("driver DSN dropped %q:\n  got: %s", want, got.Driver)
		}
	}
	if strings.Contains(got.Driver, "namespace=") {
		t.Errorf("driver DSN still contains namespace=: %s", got.Driver)
	}
}

// TestEncodeMetrics_ReturnsTheMarshalError pins the error this
// package used to discard.
//
// Save validates the signal first, and domain.Signal.Validate now
// rejects non-finite metrics, so nothing reaches encodeMetrics that
// can fail today. That is the reason to test the helper directly: the
// discarded error was invisible precisely because no test could see
// it, and the next unmarshalable value to appear in a metric bag must
// not be written as an empty column the way +Inf was.
func TestEncodeMetrics_ReturnsTheMarshalError(t *testing.T) {
	b, err := encodeMetrics(map[string]float64{"mean": math.Inf(1), "n": 12})
	if err == nil {
		t.Fatalf("encodeMetrics(+Inf) = %q, nil — want the encoding/json rejection", b)
	}
	if b != nil {
		t.Errorf("encodeMetrics returned %q alongside its error, want no bytes", b)
	}
}

// TestEncodeMetrics_RoundTripsAFiniteBag checks the helper did not
// change what a representable bag serialises to; the column contents
// are a storage contract the read path parses back.
func TestEncodeMetrics_RoundTripsAFiniteBag(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   map[string]float64
		want string
	}{
		{"populated", map[string]float64{"slope": 1.25}, `{"slope":1.25}`},
		{"empty", map[string]float64{}, `{}`},
		{"nil", nil, `null`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := encodeMetrics(tc.in)
			if err != nil {
				t.Fatalf("encodeMetrics() error = %v", err)
			}
			if string(got) != tc.want {
				t.Errorf("encodeMetrics() = %q, want %q", got, tc.want)
			}
		})
	}
}
