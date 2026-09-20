//go:build integration

// Package integration runs the cross-service smoke tests against a
// running chronos + mnemos stack. The build tag is the gate: the
// suite never runs under a plain `go test ./...`. Stand the stack up
// first (docker compose -f test/integration/docker-compose.yml up -d)
// then run:
//
//	go test -tags=integration ./test/integration/...
//
// Endpoints are taken from CHRONOS_INTEGRATION_URL /
// MNEMOS_INTEGRATION_URL with sensible localhost defaults; CI can
// repoint them at a hosted instance without code changes.
package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"
)

func chronosBaseURL() string {
	if v := os.Getenv("CHRONOS_INTEGRATION_URL"); v != "" {
		return v
	}
	return "http://127.0.0.1:7778"
}

func mnemosBaseURL() string {
	if v := os.Getenv("MNEMOS_INTEGRATION_URL"); v != "" {
		return v
	}
	return "http://127.0.0.1:7777"
}

// TestHealthBothServices is the boot-time check: both services
// answer /health 200 before any of the cross-talk tests fire.
// Failing here means the docker stack isn't up — fail fast with a
// clear message rather than hitting confusing 500s downstream.
func TestHealthBothServices(t *testing.T) {
	for name, url := range map[string]string{
		"chronos": chronosBaseURL() + "/health",
		"mnemos":  mnemosBaseURL() + "/health",
	} {
		if err := waitForHealth(url, 30*time.Second); err != nil {
			t.Fatalf("%s health: %v", name, err)
		}
	}
}

// TestCrossTalk_IngestThenList smoke-tests the chronos write path
// end-to-end: ingest one observation, then list signals for the
// scope and verify the request succeeds. Detection is async, so the
// list may legitimately return zero rows — the assertion is that
// the HTTP contracts work, not that detection has fired.
func TestCrossTalk_IngestThenList(t *testing.T) {
	scope := newUUID()
	entity := newUUID()
	body := map[string]any{
		"entity_id": entity,
		"scope_id":  scope,
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		"features":  []float64{1.0, 2.0, 3.0, 5.0},
		"adapter":   "integration",
	}
	postJSON(t, chronosBaseURL()+"/v1/ingest", body, http.StatusAccepted)

	resp := getJSON(t, fmt.Sprintf("%s/v1/signals?scope_id=%s", chronosBaseURL(), scope))
	if _, ok := resp["signals"]; !ok {
		t.Fatalf("missing 'signals' key in response: %+v", resp)
	}
}

// TestCrossTalk_MnemosEpisodeRoundTrip pins the mnemos write+read
// path. Appends an episode, then reads the episode list back and
// verifies the seeded row surfaces.
//
// The endpoint is /v1/episodes, not /v1/events. mnemos renamed the
// route, the request field and the response key together; this test
// was written against the old names and had been failing ever since.
// Diagnosing it is easy to get wrong, because the auth middleware
// short-circuits before the router: unauthenticated, the dead route
// answers 401 "missing bearer token", which reads like a pure auth
// problem and hides the 404 underneath. Both had to be fixed, and
// the 401 is the one you see first.
func TestCrossTalk_MnemosEpisodeRoundTrip(t *testing.T) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	runID := "integration:" + newUUID()
	token := mnemosToken(t)

	postJSON(t, mnemosBaseURL()+"/v1/episodes", map[string]any{
		"episodes": []map[string]any{{
			"id":              "ev_smoke_" + newUUID(),
			"run_id":          runID,
			"schema_version":  "v1",
			"content":         "integration smoke event",
			"source_input_id": "smoke",
			"timestamp":       now,
			"ingested_at":     now,
		}},
	}, http.StatusCreated, token)

	resp := getJSON(t, fmt.Sprintf("%s/v1/episodes?run_id=%s", mnemosBaseURL(), runID), token)
	episodes, ok := resp["episodes"].([]any)
	if !ok || len(episodes) != 1 {
		t.Fatalf("expected 1 episode under %s, got %+v", runID, resp)
	}
}

// mnemosToken returns the bearer token for the mnemos API. mnemos is
// secure by default -- every /v1 request needs a token, reads
// included -- so there is no anonymous mode to fall back to.
//
// Fails rather than skips when unset. A skip here is how this suite
// would go quietly green while testing nothing, which is the failure
// mode this file already lived through.
func mnemosToken(t *testing.T) string {
	t.Helper()
	tok := os.Getenv("MNEMOS_INTEGRATION_TOKEN")
	if tok == "" {
		t.Fatal("MNEMOS_INTEGRATION_TOKEN is unset. Mint one against the compose stack:\n" +
			"  docker compose -f test/integration/docker-compose.yml exec -T mnemos \\\n" +
			"    mnemos user create --name smoke --email smoke@integration.local --scope events:write\n" +
			"  docker compose -f test/integration/docker-compose.yml exec -T mnemos \\\n" +
			"    mnemos token issue --user <user_id> --ttl 1h")
	}
	return tok
}

func newUUID() string {
	// crypto/rand-backed UUIDv4. Hand-rolled here so the smoke test
	// doesn't add a runtime dep on github.com/google/uuid for the
	// build-tagged binary that ships with chronos.
	b := make([]byte, 16)
	if _, err := readRandom(b); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func waitForHealth(url string, total time.Duration) error {
	deadline := time.Now().Add(total)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil && resp.StatusCode == 200 {
			_ = resp.Body.Close()
			return nil
		}
		if resp != nil {
			_ = resp.Body.Close()
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("%s never returned 200 within %s", url, total)
}

// postJSON and getJSON take the bearer token as a variadic trailing
// argument: chronos needs no auth on these routes and passes none,
// mnemos requires one on every request. Variadic rather than a plain
// parameter so the chronos call sites stay free of a "" that would
// read as "no token needed here" in one place and "token forgotten"
// in another.
func postJSON(t *testing.T, url string, body any, expectStatus int, token ...string) {
	t.Helper()
	buf, _ := json.Marshal(body)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(buf))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	req.Header.Set("Content-Type", "application/json")
	setBearer(req, token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != expectStatus {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST %s status = %d, want %d (body=%s)", url, resp.StatusCode, expectStatus, string(raw))
	}
}

func getJSON(t *testing.T, url string, token ...string) map[string]any {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	setBearer(req, token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != 200 {
		raw, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET %s status = %d (body=%s)", url, resp.StatusCode, string(raw))
	}
	out := map[string]any{}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}
	return out
}

func setBearer(req *http.Request, token []string) {
	if len(token) > 0 && token[0] != "" {
		req.Header.Set("Authorization", "Bearer "+token[0])
	}
}
