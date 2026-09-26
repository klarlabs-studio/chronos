// Package observability exposes Chronos's runtime metrics in
// Prometheus exposition format. It is intentionally small —
// counters only, hand-rolled rendering, no external dependencies.
//
// Two consumers feed the registry:
//
//   - The pipeline package calls ObserveSignal whenever a detector
//     emits and ObserveObservations whenever entity states are saved.
//   - The HTTP middleware calls ObserveHTTP for every request once
//     the response has been written.
//
// The /metrics endpoint (mounted by internal/api) renders the current
// state in Prometheus text format. Operators wanting more
// sophisticated instrumentation (histograms, exemplars, OTLP) should
// front Chronos with a sidecar; this in-process implementation is
// the operational floor, not the ceiling.
package observability

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

// Metrics is a thread-safe in-memory metrics registry. The zero value
// is unusable; construct with [New].
type Metrics struct {
	mu sync.RWMutex

	// All maps are labelKey → value. labelKey encodes the label set as
	// a stable string (see makeLabelKey) so we can scan deterministically.
	signalsEmitted    map[labelKey]uint64
	observationsTotal map[labelKey]uint64
	httpRequestsTotal map[labelKey]uint64
	httpDurationSum   map[labelKey]float64
	httpDurationCount map[labelKey]uint64
	webhookDeliveries map[labelKey]uint64

	detectorDurationSum   map[labelKey]float64
	detectorDurationCount map[labelKey]uint64
	detectorSignals       map[labelKey]uint64
	detectorSkips         map[labelKey]uint64
	signalsTruncated      map[labelKey]uint64

	scheduler schedulerHealth
}

// labelKey is a string in the form "k1=v1,k2=v2" with keys sorted
// ascending. It is small enough to be a map key but preserves the
// label values so we can render them later.
type labelKey string

// New creates an empty registry.
func New() *Metrics {
	return &Metrics{
		signalsEmitted:        make(map[labelKey]uint64),
		observationsTotal:     make(map[labelKey]uint64),
		httpRequestsTotal:     make(map[labelKey]uint64),
		httpDurationSum:       make(map[labelKey]float64),
		httpDurationCount:     make(map[labelKey]uint64),
		webhookDeliveries:     make(map[labelKey]uint64),
		detectorDurationSum:   make(map[labelKey]float64),
		detectorDurationCount: make(map[labelKey]uint64),
		detectorSignals:       make(map[labelKey]uint64),
		detectorSkips:         make(map[labelKey]uint64),
		signalsTruncated:      make(map[labelKey]uint64),
	}
}

// ObserveSignal records that one signal of the given pattern was
// emitted.
func (m *Metrics) ObserveSignal(pattern string) {
	if m == nil {
		return
	}
	k := makeLabelKey("pattern", pattern)
	m.mu.Lock()
	m.signalsEmitted[k]++
	m.mu.Unlock()
}

// ObserveObservations records that n observations were ingested from
// the named adapter.
func (m *Metrics) ObserveObservations(adapter string, n int) {
	if m == nil || n <= 0 {
		return
	}
	k := makeLabelKey("adapter", adapter)
	m.mu.Lock()
	m.observationsTotal[k] += uint64(n)
	m.mu.Unlock()
}

// ObserveWebhook records the outcome of a single webhook delivery
// attempt. outcome is one of "success", "client_error", "failure";
// status is the final HTTP status code observed (0 when the request
// never returned, e.g., DNS failure).
func (m *Metrics) ObserveWebhook(outcome string, status int) {
	if m == nil {
		return
	}
	k := makeLabelKey("outcome", outcome, "status", fmt.Sprintf("%d", status))
	m.mu.Lock()
	m.webhookDeliveries[k]++
	m.mu.Unlock()
}

// ObserveDetector records one detector invocation: wall time, how
// many signals it emitted, and whether it was a skip (zero signals).
func (m *Metrics) ObserveDetector(pattern string, dur time.Duration, emitted int) {
	if m == nil {
		return
	}
	k := makeLabelKey("pattern", pattern)
	m.mu.Lock()
	m.detectorDurationSum[k] += dur.Seconds()
	m.detectorDurationCount[k]++
	if emitted > 0 {
		m.detectorSignals[k] += uint64(emitted)
	} else {
		m.detectorSkips[k]++
	}
	m.mu.Unlock()
}

// ObserveDetectorTruncated records signals dropped by MaxSignalsPerRun.
func (m *Metrics) ObserveDetectorTruncated(pattern string) {
	if m == nil {
		return
	}
	k := makeLabelKey("pattern", pattern)
	m.mu.Lock()
	m.signalsTruncated[k]++
	m.mu.Unlock()
}

// ObserveHTTP records the outcome of an HTTP request. method and
// status are coarse labels; path is included separately on the
// duration counters because high-cardinality paths can blow up
// label space and we keep the request count cardinality bounded.
func (m *Metrics) ObserveHTTP(method string, status int, path string, dur time.Duration) {
	if m == nil {
		return
	}
	reqK := makeLabelKey("method", method, "status", fmt.Sprintf("%d", status))
	durK := makeLabelKey("path", normalisePath(path))
	m.mu.Lock()
	m.httpRequestsTotal[reqK]++
	m.httpDurationSum[durK] += dur.Seconds()
	m.httpDurationCount[durK]++
	m.mu.Unlock()
}

// Render writes the registry as a Prometheus exposition document.
// Output is sorted within each family so diffs across scrapes stay
// stable.
func (m *Metrics) Render(w io.Writer) error {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if err := writeFamily(w, "chronos_signals_emitted_total", "counter",
		"Total signals emitted by the engine, labelled by pattern.",
		mapToSorted(m.signalsEmitted)); err != nil {
		return err
	}
	if err := writeFamily(w, "chronos_observations_total", "counter",
		"Total entity-state observations ingested, labelled by adapter.",
		mapToSorted(m.observationsTotal)); err != nil {
		return err
	}
	if err := writeFamily(w, "chronos_http_requests_total", "counter",
		"Total HTTP requests served, labelled by method and status.",
		mapToSorted(m.httpRequestsTotal)); err != nil {
		return err
	}
	if err := writeFamily(w, "chronos_http_request_duration_seconds_sum", "counter",
		"Cumulative HTTP request duration in seconds, labelled by path.",
		mapToSortedFloats(m.httpDurationSum)); err != nil {
		return err
	}
	if err := writeFamily(w, "chronos_http_request_duration_seconds_count", "counter",
		"Number of HTTP requests, labelled by path. Divide _sum by _count for mean latency.",
		mapToSorted(m.httpDurationCount)); err != nil {
		return err
	}
	if err := writeFamily(w, "chronos_webhook_deliveries_total", "counter",
		"Total webhook delivery attempts, labelled by outcome (success|client_error|failure) and final status code.",
		mapToSorted(m.webhookDeliveries)); err != nil {
		return err
	}
	if err := writeFamily(w, "chronos_detector_duration_seconds_sum", "counter",
		"Cumulative detector wall time in seconds, labelled by pattern.",
		mapToSortedFloats(m.detectorDurationSum)); err != nil {
		return err
	}
	if err := writeFamily(w, "chronos_detector_duration_seconds_count", "counter",
		"Number of detector invocations, labelled by pattern. Divide _sum by _count for mean latency.",
		mapToSorted(m.detectorDurationCount)); err != nil {
		return err
	}
	if err := writeFamily(w, "chronos_detector_signals_total", "counter",
		"Signals produced by detectors before the MaxSignalsPerRun cap, labelled by pattern.",
		mapToSorted(m.detectorSignals)); err != nil {
		return err
	}
	if err := writeFamily(w, "chronos_detector_skips_total", "counter",
		"Detector invocations that emitted zero signals, labelled by pattern.",
		mapToSorted(m.detectorSkips)); err != nil {
		return err
	}
	if err := writeFamily(w, "chronos_signals_truncated_total", "counter",
		"Signals dropped by MaxSignalsPerRun after sort, labelled by pattern.",
		mapToSorted(m.signalsTruncated)); err != nil {
		return err
	}
	return m.renderScheduler(w)
}

// kvSample pairs a labelKey with a numeric value for rendering.
type kvSample struct {
	key   labelKey
	value string
}

func mapToSorted(m map[labelKey]uint64) []kvSample {
	out := make([]kvSample, 0, len(m))
	for k, v := range m {
		out = append(out, kvSample{key: k, value: fmt.Sprintf("%d", v)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key < out[j].key })
	return out
}

func mapToSortedFloats(m map[labelKey]float64) []kvSample {
	out := make([]kvSample, 0, len(m))
	for k, v := range m {
		// Prometheus exposition format mandates plain decimal or
		// scientific notation; %g produces both as needed.
		out = append(out, kvSample{key: k, value: fmt.Sprintf("%g", v)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].key < out[j].key })
	return out
}

func writeFamily(w io.Writer, name, kind, help string, samples []kvSample) error {
	if _, err := fmt.Fprintf(w, "# HELP %s %s\n", name, help); err != nil {
		return err
	}
	if _, err := fmt.Fprintf(w, "# TYPE %s %s\n", name, kind); err != nil {
		return err
	}
	for _, s := range samples {
		if _, err := fmt.Fprintf(w, "%s%s %s\n", name, renderLabels(s.key), s.value); err != nil {
			return err
		}
	}
	return nil
}

// makeLabelKey builds a labelKey from key/value pairs. Keys are
// sorted alphabetically so two callers passing the same labels in
// any order produce the same key. Empty values are kept (Prometheus
// allows them).
func makeLabelKey(kv ...string) labelKey {
	if len(kv)%2 != 0 {
		panic("makeLabelKey: odd argument count")
	}
	pairs := make([]string, 0, len(kv)/2)
	for i := 0; i < len(kv); i += 2 {
		// G602 false positive: panic above on odd argument count
		// guarantees i+1 is in range.
		pairs = append(pairs, kv[i]+"="+escapeValue(kv[i+1])) //nolint:gosec
	}
	sort.Strings(pairs)
	return labelKey(strings.Join(pairs, ","))
}

// renderLabels turns a labelKey back into a "{k=\"v\",k=\"v\"}" string.
// Returns "" for the empty label set (used when a counter has no labels).
func renderLabels(k labelKey) string {
	if k == "" {
		return ""
	}
	// Each part is `k="v"` already (escapeValue wraps quotes), so no
	// per-part transformation needed — the split halves are the
	// rendered halves.
	return "{" + string(k) + "}"
}

// escapeValue returns the value wrapped in double quotes with
// backslashes / quotes / newlines escaped as the Prometheus exposition
// format requires.
func escapeValue(v string) string {
	v = strings.ReplaceAll(v, `\`, `\\`)
	v = strings.ReplaceAll(v, `"`, `\"`)
	v = strings.ReplaceAll(v, "\n", `\n`)
	return `"` + v + `"`
}

// normalisePath collapses path-parameter segments (UUIDs, numbers)
// to the constant `:id` so the duration counters do not blow up in
// label cardinality. Today this only needs to handle /v1/signals/<id>.
func normalisePath(path string) string {
	if strings.HasPrefix(path, "/v1/signals/") && len(path) > len("/v1/signals/") {
		return "/v1/signals/:id"
	}
	return path
}
