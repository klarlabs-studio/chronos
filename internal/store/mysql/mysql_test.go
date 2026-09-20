package mysql

import (
	"math"
	"strings"
	"testing"

	"github.com/felixgeelhaar/chronos/internal/ports"
	"github.com/google/uuid"
)

// Unit tests for MySQL DSN translation. The integration test that
// exercises a real server lives in integration_test.go and is gated
// on TEST_MYSQL_DSN.

func TestBuildWhere_ScopeIDs(t *testing.T) {
	a := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	b := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	where, args := buildWhere(ports.SignalFilter{ScopeIDs: []uuid.UUID{a, b}})
	if !strings.Contains(where, "scope_id IN (?,?)") && !strings.Contains(where, "scope_id IN (?, ?)") {
		t.Errorf("where = %q, want IN clause", where)
	}
	if len(args) != 2 || args[0] != a.String() || args[1] != b.String() {
		t.Errorf("args = %v", args)
	}
}

func TestParseDSN(t *testing.T) {
	cases := []struct {
		name         string
		in           string
		wantNS       string
		wantInDriver string // substring expected in the driver DSN
		wantErr      bool
	}{
		{
			name:         "default port + namespace default",
			in:           "mysql://root:secret@host/?",
			wantNS:       "chronos",
			wantInDriver: "@tcp(host:3306)/chronos",
		},
		{
			name:         "explicit port",
			in:           "mysql://user:pw@db.local:6603/?namespace=tenant_a",
			wantNS:       "tenant_a",
			wantInDriver: "@tcp(db.local:6603)/tenant_a",
		},
		{
			name:         "mariadb alias accepted",
			in:           "mariadb://u:p@host/?",
			wantNS:       "chronos",
			wantInDriver: "@tcp(host:3306)/chronos",
		},
		{
			name:         "no userinfo defaults to root",
			in:           "mysql://host/?",
			wantNS:       "chronos",
			wantInDriver: "root@tcp(host:3306)/chronos",
		},
		{
			name:    "wrong scheme rejected",
			in:      "postgres://h/db",
			wantErr: true,
		},
		{
			name:    "invalid namespace rejected",
			in:      "mysql://h/?namespace=BAD-name",
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
			if !strings.Contains(got.DriverDSN, tc.wantInDriver) {
				t.Errorf("DriverDSN missing %q:\n  got: %s", tc.wantInDriver, got.DriverDSN)
			}
			if !strings.Contains(got.DriverDSN, "parseTime=true") {
				t.Errorf("DriverDSN missing parseTime=true:\n  got: %s", got.DriverDSN)
			}
			if !strings.Contains(got.DriverDSN, "loc=UTC") {
				t.Errorf("DriverDSN missing loc=UTC:\n  got: %s", got.DriverDSN)
			}
			// AdminDSN is the same as DriverDSN minus the database
			// name; verify the shape.
			if !strings.Contains(got.AdminDSN, "@tcp(") {
				t.Errorf("AdminDSN malformed: %s", got.AdminDSN)
			}
			if strings.Contains(got.AdminDSN, "/"+tc.wantNS+"?") {
				t.Errorf("AdminDSN should not include the namespace database: %s", got.AdminDSN)
			}
		})
	}
}

func TestSplitStatements_HandlesCommentsAndBlankLines(t *testing.T) {
	in := `
-- a comment
CREATE TABLE foo (id INT);

-- another comment
CREATE INDEX idx_foo ON foo(id);
`
	got := splitStatements(in)
	if len(got) != 2 {
		t.Fatalf("expected 2 statements, got %d: %v", len(got), got)
	}
	if !strings.Contains(got[0], "CREATE TABLE foo") {
		t.Errorf("first statement: %q", got[0])
	}
	if !strings.Contains(got[1], "CREATE INDEX idx_foo") {
		t.Errorf("second statement: %q", got[1])
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
