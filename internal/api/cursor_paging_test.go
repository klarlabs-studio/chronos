package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos/internal/domain"
	"github.com/google/uuid"
)

type signalPage struct {
	Signals    []SignalDTO `json:"signals"`
	Count      int         `json:"count"`
	NextCursor string      `json:"next_cursor"`
	HasMore    bool        `json:"has_more"`
}

func getPage(t *testing.T, rawURL string) signalPage {
	t.Helper()
	resp, err := http.Get(rawURL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status %d", rawURL, resp.StatusCode)
	}
	var p signalPage
	if err := json.NewDecoder(resp.Body).Decode(&p); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return p
}

// A consumer polling by cursor with no limit -- the deployed Vorhut
// processor -- must drain a backlog larger than one page, oldest first,
// without losing the rows that share a tick's timestamp. Before, the
// cursor returned everything since itself, newest first: a 48k-signal
// backlog timed out on every poll and the consumer stopped receiving
// detections for 9.5 hours.
func TestListSignals_SinceCursor_PagesABacklogOldestFirst(t *testing.T) {
	ts, mem := setupServer(t)
	defer ts.Close()
	scope := uuid.New()
	t0 := time.Date(2026, 9, 27, 2, 37, 0, 0, time.UTC)

	const perTick, ticks = 400, 3 // 1,200 signals; every tick one timestamp
	want := map[uuid.UUID]bool{}
	for tick := 0; tick < ticks; tick++ {
		at := t0.Add(time.Duration(tick) * 30 * time.Second)
		for i := 0; i < perTick; i++ {
			sig := seedSignal(t, "", scope, at, uuid.New(), 0.7)
			if err := mem.Signals.Save(context.Background(), sig); err != nil {
				t.Fatalf("Save: %v", err)
			}
			want[sig.ID] = true
		}
	}

	cursor := encodeSignalCursor(signalCursor{DetectedAt: t0.Add(-time.Minute)})
	seen := map[uuid.UUID]bool{}
	var last time.Time
	polls := 0
	for {
		polls++
		if polls > 10 {
			t.Fatal("cursor never reached the end of the backlog")
		}
		p := getPage(t, ts.URL+"/v1/signals?scope_id="+scope.String()+"&since_cursor="+url.QueryEscape(cursor))
		if len(p.Signals) > defaultPageLimit {
			t.Fatalf("page of %d exceeds the default page of %d", len(p.Signals), defaultPageLimit)
		}
		for _, s := range p.Signals {
			if seen[s.ID] {
				t.Fatalf("signal %s returned twice", s.ID)
			}
			seen[s.ID] = true
			if s.DetectedAt.Before(last) {
				t.Fatalf("page went backwards in time: %v after %v", s.DetectedAt, last)
			}
			last = s.DetectedAt
		}
		if p.HasMore != (len(p.Signals) == defaultPageLimit) {
			t.Fatalf("has_more=%v for a page of %d", p.HasMore, len(p.Signals))
		}
		if p.NextCursor == "" {
			break
		}
		cursor = p.NextCursor
	}
	if len(seen) != len(want) {
		t.Fatalf("drained %d of %d signals", len(seen), len(want))
	}
}

// order=asc pages forward from a plain since, so a consumer's first poll
// (no cursor yet) is bounded the same way.
func TestListSignals_OrderAsc_PagesForwardFromSince(t *testing.T) {
	ts, mem := setupServer(t)
	defer ts.Close()
	scope := uuid.New()
	t0 := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)
	var saved []domain.Signal
	for i := 0; i < 5; i++ {
		sig := seedSignal(t, "", scope, t0.Add(time.Duration(i)*time.Minute), uuid.New(), 0.7)
		if err := mem.Signals.Save(context.Background(), sig); err != nil {
			t.Fatalf("Save: %v", err)
		}
		saved = append(saved, sig)
	}

	p := getPage(t, ts.URL+"/v1/signals?scope_id="+scope.String()+"&order=asc&limit=2&since="+url.QueryEscape(t0.Add(time.Minute).Format(time.RFC3339)))
	if len(p.Signals) != 2 || p.Signals[0].ID != saved[1].ID || p.Signals[1].ID != saved[2].ID {
		t.Fatalf("order=asc page = %+v, want signals 1 and 2 oldest first", p.Signals)
	}
	if !p.HasMore {
		t.Fatal("has_more=false with rows left")
	}
	next := getPage(t, ts.URL+"/v1/signals?scope_id="+scope.String()+"&limit=2&since_cursor="+url.QueryEscape(p.NextCursor))
	if len(next.Signals) != 2 || next.Signals[0].ID != saved[3].ID {
		t.Fatalf("next page = %+v, want signals 3 and 4", next.Signals)
	}
}

func TestListSignals_RejectsUnknownOrder(t *testing.T) {
	ts, _ := setupServer(t)
	defer ts.Close()
	resp, err := http.Get(ts.URL + "/v1/signals?scope_id=" + uuid.New().String() + "&order=sideways")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status %d, want 400", resp.StatusCode)
	}
}
