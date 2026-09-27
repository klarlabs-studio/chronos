package client_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/felixgeelhaar/chronos/client"
	"github.com/google/uuid"
)

func TestClient_Health(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			t.Errorf("path = %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
	}))
	defer srv.Close()

	c, err := client.New(srv.URL)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := c.Health(context.Background()); err != nil {
		t.Fatalf("Health: %v", err)
	}
}

func TestClient_ListSignals_Filters(t *testing.T) {
	scope := uuid.New()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("scope_id") != scope.String() {
			t.Errorf("scope_id = %q", q.Get("scope_id"))
		}
		if q.Get("pattern") != client.PatternTypeRecurrence {
			t.Errorf("pattern = %q", q.Get("pattern"))
		}
		if q.Get("min_confidence") != "0.7" {
			t.Errorf("min_confidence = %q", q.Get("min_confidence"))
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"count":   1,
			"signals": []client.Signal{{ID: uuid.New(), ScopeID: scope, Pattern: client.PatternTypeRecurrence, Confidence: 0.9, DetectedAt: time.Now()}},
		})
	}))
	defer srv.Close()

	c, _ := client.New(srv.URL)
	got, err := c.Signals().Scope(scope).Pattern(client.PatternTypeRecurrence).MinConfidence(0.7).List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d", len(got))
	}
	if got[0].Pattern != client.PatternTypeRecurrence {
		t.Errorf("pattern = %s", got[0].Pattern)
	}
}

func TestClient_ListSignals_UnmarshalsExplanationAndConfidenceClass(t *testing.T) {
	scope := uuid.New()
	at := time.Date(2026, 5, 1, 12, 0, 0, 0, time.UTC)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"count": 1,
			"signals": []map[string]any{{
				"id":               uuid.New(),
				"scope_id":         scope,
				"pattern":          client.PatternTypeTrend,
				"confidence":       0.9,
				"detected_at":      at,
				"confidence_class": "established",
				"explanation": map[string]any{
					"detector_version":     "trend-v2",
					"comparable_peers":     12,
					"baseline_window_days": 90,
					"threshold_used":       2.5,
					"feature_evolution": []map[string]any{
						{"at": at, "value": 26.0},
					},
				},
			}},
		})
	}))
	defer srv.Close()

	c, _ := client.New(srv.URL)
	got, err := c.Signals().Scope(scope).List(context.Background())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("len = %d", len(got))
	}
	if got[0].ConfidenceClass != "established" {
		t.Errorf("confidence_class = %q, want established", got[0].ConfidenceClass)
	}
	if got[0].Explanation == nil {
		t.Fatal("explanation missing")
	}
	if got[0].Explanation.DetectorVersion != "trend-v2" {
		t.Errorf("detector_version = %q", got[0].Explanation.DetectorVersion)
	}
	if got[0].Explanation.ComparablePeers != 12 {
		t.Errorf("comparable_peers = %d", got[0].Explanation.ComparablePeers)
	}
}

func TestClient_Ingest(t *testing.T) {
	id := uuid.New()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/ingest" {
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "accepted", "id": id})
	}))
	defer srv.Close()

	c, _ := client.New(srv.URL)
	gotID, err := c.Ingest(context.Background(), client.IngestRequest{
		EntityID:  uuid.New(),
		ScopeID:   uuid.New(),
		Features:  []float64{1, 2, 3, 5},
		Adapter:   "test",
		Timestamp: time.Now(),
	})
	if err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if gotID != id {
		t.Errorf("id = %s, want %s", gotID, id)
	}
}

func TestClient_IngestBatchAndListPage(t *testing.T) {
	scope := uuid.New()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/v1/ingest/batch":
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(client.IngestBatchResponse{Accepted: 2})
		case r.Method == http.MethodGet && r.URL.Path == "/v1/signals":
			if r.URL.Query().Get("since_cursor") != "tok" {
				t.Errorf("since_cursor = %q", r.URL.Query().Get("since_cursor"))
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"count":       1,
				"next_cursor": "next",
				"signals":     []client.Signal{{ID: uuid.New(), ScopeID: scope, Pattern: client.PatternTypeStall}},
			})
		default:
			t.Errorf("unexpected %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c, _ := client.New(srv.URL)
	batch, err := c.IngestBatch(context.Background(), []client.IngestRequest{
		{EntityID: uuid.New(), ScopeID: scope, Features: []float64{1, 2}},
		{EntityID: uuid.New(), ScopeID: scope, Features: []float64{1, 3}},
	})
	if err != nil {
		t.Fatalf("IngestBatch: %v", err)
	}
	if batch.Accepted != 2 {
		t.Errorf("accepted = %d", batch.Accepted)
	}
	page, err := c.Signals().Scope(scope).SinceCursor("tok").ListPage(context.Background())
	if err != nil {
		t.Fatalf("ListPage: %v", err)
	}
	if page.NextCursor != "next" || page.Count != 1 {
		t.Errorf("page = %+v", page)
	}
}

func TestClient_ScopesQuery(t *testing.T) {
	a, b := uuid.New(), uuid.New()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := r.URL.Query().Get("scope_in")
		want := a.String() + "," + b.String()
		if got != want {
			t.Errorf("scope_in = %q, want %q", got, want)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"count": 0, "signals": []client.Signal{}})
	}))
	defer srv.Close()
	c, _ := client.New(srv.URL)
	if _, err := c.Signals().Scopes(a, b).List(context.Background()); err != nil {
		t.Fatalf("List: %v", err)
	}
}

func TestClient_FederationExport(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/federation/export" {
			t.Errorf("path = %s", r.URL.Path)
		}
		_ = json.NewEncoder(w).Encode(client.FederationExport{Source: "chronos", Version: "v1", TotalSignals: 3})
	}))
	defer srv.Close()
	c, _ := client.New(srv.URL)
	got, err := c.FederationExport(context.Background())
	if err != nil {
		t.Fatalf("FederationExport: %v", err)
	}
	if got.Source != "chronos" || got.TotalSignals != 3 {
		t.Errorf("export = %+v", got)
	}
}

func TestClient_AuthAndError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer s3cret" {
			t.Errorf("Authorization = %q", got)
		}
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()

	c, _ := client.New(srv.URL, client.WithToken("s3cret"))
	_, err := c.Signals().Scope(uuid.New()).List(context.Background())
	var apiErr *client.APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("error = %v, want APIError", err)
	}
	if apiErr.Status != http.StatusInternalServerError {
		t.Errorf("status = %d", apiErr.Status)
	}
	if !strings.Contains(apiErr.Body, "boom") {
		t.Errorf("body = %q", apiErr.Body)
	}
}

func TestSignalQuery_RequiresScope(t *testing.T) {
	c, _ := client.New("http://127.0.0.1:1")
	if _, err := c.Signals().List(context.Background()); err == nil {
		t.Errorf("expected error for missing scope")
	}
}

func TestSignalQuery_AscendingSendsOrderAndReadsHasMore(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("order"); got != "asc" {
			t.Errorf("order = %q, want asc", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"count": 0, "signals": []client.Signal{}, "has_more": true})
	}))
	defer srv.Close()

	c, _ := client.New(srv.URL)
	page, err := c.Signals().Scope(uuid.New()).Since(time.Now().Add(-time.Hour)).Ascending().ListPage(context.Background())
	if err != nil {
		t.Fatalf("ListPage: %v", err)
	}
	if !page.HasMore {
		t.Fatal("HasMore not decoded")
	}
}
