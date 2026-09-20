package sqlite

import (
	"testing"
	"time"
)

func TestFormatTime_FixedWidthFractional(t *testing.T) {
	cases := []struct {
		name string
		t    time.Time
		want string
	}{
		{
			name: "100ms",
			t:    time.Date(2026, 1, 1, 12, 0, 0, 100_000_000, time.UTC),
			want: "2026-01-01T12:00:00.100000000Z",
		},
		{
			name: "120ms",
			t:    time.Date(2026, 1, 1, 12, 0, 0, 120_000_000, time.UTC),
			want: "2026-01-01T12:00:00.120000000Z",
		},
		{
			name: "200ms",
			t:    time.Date(2026, 1, 1, 12, 0, 0, 200_000_000, time.UTC),
			want: "2026-01-01T12:00:00.200000000Z",
		},
		{
			name: "whole second",
			t:    time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
			want: "2026-01-01T12:00:00.000000000Z",
		},
	}
	for _, tc := range cases {
		got := formatTime(tc.t)
		if got != tc.want {
			t.Errorf("%s: formatTime = %q, want %q", tc.name, got, tc.want)
		}
	}
	// The lexical order that used to fail under RFC3339Nano.
	a, b, c := formatTime(cases[0].t), formatTime(cases[1].t), formatTime(cases[2].t)
	if !(a < b && b < c) {
		t.Errorf("lexical order %q < %q < %q does not hold", a, b, c)
	}
}

func TestNormalizeTimestampText_RewritesTrimmedRFC3339Nano(t *testing.T) {
	c, err := Open(":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	// Insert a legacy trimmed form that sorts wrong under byte order.
	trimmed := "2026-01-01T12:00:00.1Z" // 100ms, trailing zeros stripped
	fixed120 := "2026-01-01T12:00:00.120000000Z"
	id1, id2 := "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
	_, err = c.DB.Exec(`INSERT INTO entity_states (id, entity_id, scope_id, timestamp, features, adapter, created_at)
		VALUES (?, 'e', 's', ?, '[1]', 'test', ?),
		       (?, 'e', 's', ?, '[1]', 'test', ?)`,
		id1, trimmed, formatTime(time.Now()),
		id2, fixed120, formatTime(time.Now()))
	if err != nil {
		t.Fatal(err)
	}
	if err := normalizeTimestampText(c.DB); err != nil {
		t.Fatal(err)
	}
	var got string
	if err := c.DB.QueryRow(`SELECT timestamp FROM entity_states WHERE id = ?`, id1).Scan(&got); err != nil {
		t.Fatal(err)
	}
	want := "2026-01-01T12:00:00.100000000Z"
	if got != want {
		t.Errorf("normalized = %q, want %q", got, want)
	}
	if !(got < fixed120) {
		t.Errorf("after normalize %q should sort before %q", got, fixed120)
	}
}
