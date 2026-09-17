// Package config provides Chronos configuration. All knobs are env-var
// driven and read once at startup; the rest of the engine receives a
// *Config explicitly so global mutable state stays out of the codebase.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds all Chronos configuration. Tier-A knobs (DB, recurrence,
// HTTP) live in this top-level struct; Tier-B detector knobs are added
// next to the Tier-A fields as more detectors land.
type Config struct {
	// Database — DBDSN is the new primary entry point (Mnemos
	// ADR 0001 contract). DBType + DBConnStr are kept as a legacy
	// alias so existing operator configurations keep working through
	// the cutover; if DBDSN is empty at startup, the runtime
	// translates the legacy pair into a DSN form.
	DBDSN     string // e.g. sqlite:///chronos.db, postgres://user:pw@host/db?namespace=chronos
	DBType    string // legacy: sqlite, postgres, memory
	DBConnStr string // legacy: connection string or path

	// Detection — common
	MaxSignalsPerRun   int           // Limit signals per detect run; 0 = unlimited
	ComputationTimeout time.Duration // Overall compute job timeout

	// Detection — Recurrence
	SimilarityThreshold float64 // Minimum cosine similarity (0..1)
	MinSampleSize       int     // Minimum peer cases to emit a recurrence signal

	// Detection — Trend (Tier B)
	TrendMinSlope  float64 // Minimum |slope| of normalised regression
	TrendMinPoints int     // Minimum points to consider a trend

	// Detection — Spike / Drop (Tier B)
	SpikeZScore float64 // Absolute z-score for a positive deviation
	DropZScore  float64 // Absolute z-score for a negative deviation
	SpikeWindow int     // Window size (in points) for the rolling baseline

	// Detection — Stall (Tier B)
	StallMaxStdDev float64 // Maximum stddev of normalised outcome to qualify as stalled
	StallMinPoints int     // Minimum points to consider a stall

	// Detection — Anomaly (cross-entity, the dual of Recurrence)
	AnomalyMaxSimilarity float64 // Max similarity to nearest peer to count as anomalous
	AnomalyMinPeers      int     // Minimum peers required for cross-entity comparison

	// Detection — Seasonality (autocorrelation)
	SeasonalityMinAutocorr float64 // Minimum autocorrelation at any lag to emit
	SeasonalityMinPoints   int     // Minimum observations to consider seasonality
	SeasonalityMinPeriod   int     // Minimum lag (period) to consider

	// Detection — Correlation (cross-series Pearson)
	CorrelationMin       float64 // Minimum |Pearson r| to emit
	CorrelationMinPoints int     // Minimum aligned observations between two series

	// Detection — ChangePoint (best-split mean-shift)
	ChangePointMinShift  float64 // Minimum |Δmean / pooled_stddev| to emit
	ChangePointMinPoints int     // Minimum points required (split needs ≥ 2 either side)
	// ChangePointMinDelta is a minimum |Δmean| in the outcome's own
	// units, applied alongside the standardised shift. Zero disables it,
	// which is the default and the historical behaviour.
	//
	// The standardised test divides by pooled stddev, so on a series that
	// barely varies any movement at all scores as a large shift. That is
	// correct — the change is real and statistically unmistakable — but
	// on a bounded outcome it reports changes too small to act on. This
	// floor lets a deployment say how much movement is worth a signal.
	ChangePointMinDelta float64

	// Detection — OutlierCluster (cohort-level anomaly clusters)
	OutlierClusterMinSeries  int           // Minimum series sharing a cluster
	OutlierClusterZ          float64       // |z-score| threshold to count a series as anomalous at a tick
	OutlierClusterTimeWindow time.Duration // sliding-window width for "around the same time"

	// Detection — CrossScopeCorrelation
	CrossScopeMin       float64 // Minimum |Pearson r| to emit (cross-scope tightness)
	CrossScopeMinPoints int     // Minimum aligned observations between two series

	// ConfidenceClassEstablished is the MIN_POINTS multiplier that a
	// signal's supporting sample size must clear to be labelled
	// "established" instead of "tentative". 2.0 by default — a
	// signal with twice the floor's worth of observations is no
	// longer borderline.
	ConfidenceClassEstablished float64

	// ConfidenceClassStrong is the MIN_POINTS multiplier that a
	// signal's supporting sample size must clear to be labelled
	// "strong". 5.0 by default — well into the part of the curve
	// where increasing n stops materially changing the conclusion.
	ConfidenceClassStrong float64

	// AnonymizeCrossScope strips identifying scope/series ids from
	// CrossScopeCorrelation signals when true. The detector still
	// runs and the statistical fields (r, n, direction) survive; the
	// scope_id, series, and evidence.series fields are replaced with
	// deterministic UUIDv5 hashes so a population-level signal stays
	// useful without exposing which tenants paired up. Off by default
	// — flip on for deployments where cross-tenant detectors are
	// otherwise unsafe (set CHRONOS_ANONYMIZE_CROSS_SCOPE=true).
	AnonymizeCrossScope bool

	// DetectorParallelism enables parallel execution of per-scope
	// detectors. Off by default; flip on for deployments with many
	// detectors and many scopes where wall-clock matters more than
	// deterministic signal ordering.
	DetectorParallelism bool

	// HTTP API
	HTTPPort int
	HTTPHost string
	APIToken string // optional bearer token; empty disables auth on the API

	// gRPC API
	GRPCPort int    // 0 disables the gRPC server
	GRPCHost string // defaults to "" (all interfaces)

	// Notifications — Webhooks
	WebhookURLs    []string      // comma-separated POST endpoints; empty disables webhooks
	WebhookSecret  string        // HMAC-SHA256 key for X-Chronos-Signature
	WebhookTimeout time.Duration // per-request timeout
	WebhookRetries int           // best-effort retries on 5xx (no retry on 2xx/4xx)

	// Notifications — Detection scheduler (serve only)
	DetectionInterval time.Duration // 0 disables the in-process scheduler

	// SignalRetention bounds how long detected signals are kept; 0
	// keeps them forever, which is the historical behaviour.
	//
	// A running scheduler appends rather than revises: a detector
	// derives its analysis window from the observations in front of it,
	// so on a live stream the window slides forward and the duplicate
	// check — which keys on that window — correctly finds no match.
	// Every tick is therefore a new row, and the store outlives the
	// process, so restarting reclaims nothing. Long-lived deployments
	// should set this.
	SignalRetention time.Duration
}

// Default returns sensible defaults.
func Default() *Config {
	return &Config{
		DBDSN:     defaultEnv("CHRONOS_DB_DSN", ""),
		DBType:    defaultEnv("CHRONOS_DB_TYPE", "sqlite"),
		DBConnStr: defaultEnv("CHRONOS_DB_CONN", "chronos.db"),

		MaxSignalsPerRun:   defaultEnvInt("CHRONOS_MAX_SIGNALS", 100),
		ComputationTimeout: defaultEnvDuration("CHRONOS_JOB_TIMEOUT", 10*time.Minute),

		SimilarityThreshold: defaultEnvFloat64("CHRONOS_SIM_THRESHOLD", 0.85),
		MinSampleSize:       defaultEnvInt("CHRONOS_MIN_SAMPLE", 2),

		TrendMinSlope:  defaultEnvFloat64("CHRONOS_TREND_MIN_SLOPE", 0.05),
		TrendMinPoints: defaultEnvInt("CHRONOS_TREND_MIN_POINTS", 4),

		SpikeZScore: defaultEnvFloat64("CHRONOS_SPIKE_Z", 2.5),
		DropZScore:  defaultEnvFloat64("CHRONOS_DROP_Z", 2.5),
		SpikeWindow: defaultEnvInt("CHRONOS_SPIKE_WINDOW", 5),

		StallMaxStdDev: defaultEnvFloat64("CHRONOS_STALL_MAX_STDDEV", 0.05),
		StallMinPoints: defaultEnvInt("CHRONOS_STALL_MIN_POINTS", 4),

		AnomalyMaxSimilarity: defaultEnvFloat64("CHRONOS_ANOMALY_MAX_SIM", 0.5),
		AnomalyMinPeers:      defaultEnvInt("CHRONOS_ANOMALY_MIN_PEERS", 2),

		SeasonalityMinAutocorr: defaultEnvFloat64("CHRONOS_SEASONALITY_MIN_AUTOCORR", 0.5),
		SeasonalityMinPoints:   defaultEnvInt("CHRONOS_SEASONALITY_MIN_POINTS", 12),
		SeasonalityMinPeriod:   defaultEnvInt("CHRONOS_SEASONALITY_MIN_PERIOD", 2),

		CorrelationMin:       defaultEnvFloat64("CHRONOS_CORRELATION_MIN", 0.7),
		CorrelationMinPoints: defaultEnvInt("CHRONOS_CORRELATION_MIN_POINTS", 5),

		ChangePointMinShift:  defaultEnvFloat64("CHRONOS_CHANGEPOINT_MIN_SHIFT", 1.5),
		ChangePointMinPoints: defaultEnvInt("CHRONOS_CHANGEPOINT_MIN_POINTS", 8),
		ChangePointMinDelta:  defaultEnvFloat64("CHRONOS_CHANGEPOINT_MIN_DELTA", 0),

		OutlierClusterMinSeries:  defaultEnvInt("CHRONOS_OUTLIER_CLUSTER_MIN_SERIES", 3),
		OutlierClusterZ:          defaultEnvFloat64("CHRONOS_OUTLIER_CLUSTER_Z", 2.5),
		OutlierClusterTimeWindow: defaultEnvDuration("CHRONOS_OUTLIER_CLUSTER_WINDOW", 5*time.Minute),

		CrossScopeMin:       defaultEnvFloat64("CHRONOS_CROSS_SCOPE_MIN", 0.8),
		CrossScopeMinPoints: defaultEnvInt("CHRONOS_CROSS_SCOPE_MIN_POINTS", 5),
		AnonymizeCrossScope: defaultEnvBool("CHRONOS_ANONYMIZE_CROSS_SCOPE", false),

		ConfidenceClassEstablished: defaultEnvFloat64("CHRONOS_CONFIDENCE_ESTABLISHED", 2.0),
		ConfidenceClassStrong:      defaultEnvFloat64("CHRONOS_CONFIDENCE_STRONG", 5.0),

		DetectorParallelism: defaultEnvBool("CHRONOS_DETECTOR_PARALLELISM", false),

		HTTPPort: defaultEnvInt("CHRONOS_HTTP_PORT", 7778),
		HTTPHost: defaultEnv("CHRONOS_HTTP_HOST", "127.0.0.1"),
		APIToken: defaultEnv("CHRONOS_API_TOKEN", ""),

		GRPCPort: defaultEnvInt("CHRONOS_GRPC_PORT", 0),
		GRPCHost: defaultEnv("CHRONOS_GRPC_HOST", ""),

		WebhookURLs:    defaultEnvSlice("CHRONOS_WEBHOOK_URLS", nil),
		WebhookSecret:  defaultEnv("CHRONOS_WEBHOOK_SECRET", ""),
		WebhookTimeout: defaultEnvDuration("CHRONOS_WEBHOOK_TIMEOUT", 5*time.Second),
		WebhookRetries: defaultEnvInt("CHRONOS_WEBHOOK_RETRIES", 1),

		DetectionInterval: defaultEnvDuration("CHRONOS_DETECTION_INTERVAL", 0),
		SignalRetention:   defaultEnvDuration("CHRONOS_SIGNAL_RETENTION", 0),
	}
}

// Validate checks configuration sanity.
func (c *Config) Validate() error {
	// At least one of the two configuration paths must be set: the
	// new DBDSN, or the legacy DBType+DBConnStr pair. The cmd entry
	// points translate the legacy form to a DSN before calling
	// store.Open, so by the time the engine sees the config one
	// will have been populated.
	if c.DBDSN == "" {
		if c.DBType == "" {
			return fmt.Errorf("db_type is required (or set CHRONOS_DB_DSN)")
		}
		if c.DBConnStr == "" {
			return fmt.Errorf("db_conn is required (or set CHRONOS_DB_DSN)")
		}
	}
	if c.SimilarityThreshold < 0 || c.SimilarityThreshold > 1 {
		return fmt.Errorf("similarity threshold must be between 0 and 1, got %f", c.SimilarityThreshold)
	}
	if c.MinSampleSize < 1 {
		return fmt.Errorf("min sample size must be at least 1, got %d", c.MinSampleSize)
	}
	if c.SpikeZScore < 0 {
		return fmt.Errorf("spike z-score must be >= 0, got %f", c.SpikeZScore)
	}
	if c.DropZScore < 0 {
		return fmt.Errorf("drop z-score must be >= 0, got %f", c.DropZScore)
	}
	if c.StallMaxStdDev < 0 {
		return fmt.Errorf("stall max stddev must be >= 0, got %f", c.StallMaxStdDev)
	}
	if c.AnomalyMaxSimilarity < -1 || c.AnomalyMaxSimilarity > 1 {
		return fmt.Errorf("anomaly max similarity must be in [-1, 1], got %f", c.AnomalyMaxSimilarity)
	}
	if c.SeasonalityMinAutocorr < -1 || c.SeasonalityMinAutocorr > 1 {
		return fmt.Errorf("seasonality min autocorrelation must be in [-1, 1], got %f", c.SeasonalityMinAutocorr)
	}
	if c.CorrelationMin < 0 || c.CorrelationMin > 1 {
		return fmt.Errorf("correlation min must be in [0, 1], got %f", c.CorrelationMin)
	}
	if c.ChangePointMinShift < 0 {
		return fmt.Errorf("changepoint min shift must be >= 0, got %f", c.ChangePointMinShift)
	}
	if c.OutlierClusterZ < 0 {
		return fmt.Errorf("outlier cluster z must be >= 0, got %f", c.OutlierClusterZ)
	}
	if c.CrossScopeMin < 0 || c.CrossScopeMin > 1 {
		return fmt.Errorf("cross-scope min must be in [0, 1], got %f", c.CrossScopeMin)
	}
	if c.ConfidenceClassEstablished < 0 {
		return fmt.Errorf("confidence established multiplier must be >= 0, got %f", c.ConfidenceClassEstablished)
	}
	if c.ConfidenceClassStrong < 0 {
		return fmt.Errorf("confidence strong multiplier must be >= 0, got %f", c.ConfidenceClassStrong)
	}
	return nil
}

func defaultEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func defaultEnvInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func defaultEnvFloat64(key string, fallback float64) float64 {
	if v := os.Getenv(key); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return fallback
}

func defaultEnvSlice(key string, fallback []string) []string {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if trimmed := strings.TrimSpace(p); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	if len(out) == 0 {
		return fallback
	}
	return out
}

func defaultEnvDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

func defaultEnvBool(key string, fallback bool) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(key))) {
	case "":
		return fallback
	case "1", "true", "yes", "on":
		return true
	case "0", "false", "no", "off":
		return false
	}
	return fallback
}
