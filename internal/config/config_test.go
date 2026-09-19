package config

import (
	"strings"
	"testing"
	"time"
)

func TestDefault_FallsBackToDefaultsWhenEnvUnset(t *testing.T) {
	// t.Setenv unsets after the test; Default reads via os.Getenv.
	t.Setenv("CHRONOS_DB_TYPE", "")
	t.Setenv("CHRONOS_DB_CONN", "")

	c := Default()
	if c.DBType != "sqlite" {
		t.Errorf("DBType default = %q, want sqlite", c.DBType)
	}
	if c.DBConnStr != "chronos.db" {
		t.Errorf("DBConnStr default = %q, want chronos.db", c.DBConnStr)
	}
	if c.MaxSignalsPerRun != 100 {
		t.Errorf("MaxSignalsPerRun = %d, want 100", c.MaxSignalsPerRun)
	}
	if c.ComputationTimeout != 10*time.Minute {
		t.Errorf("ComputationTimeout = %v, want 10m", c.ComputationTimeout)
	}
	if c.SimilarityThreshold != 0.85 {
		t.Errorf("SimilarityThreshold = %v, want 0.85", c.SimilarityThreshold)
	}
	if c.MinSampleSize != 2 {
		t.Errorf("MinSampleSize = %d, want 2", c.MinSampleSize)
	}
	if c.HTTPPort != 7778 {
		t.Errorf("HTTPPort = %d, want 7778", c.HTTPPort)
	}
	if c.HTTPHost != "127.0.0.1" {
		t.Errorf("HTTPHost = %q, want 127.0.0.1", c.HTTPHost)
	}
	if c.WebhookTimeout != 5*time.Second {
		t.Errorf("WebhookTimeout = %v, want 5s", c.WebhookTimeout)
	}
	if c.WebhookRetries != 1 {
		t.Errorf("WebhookRetries = %d, want 1", c.WebhookRetries)
	}
	if c.DetectionInterval != 0 {
		t.Errorf("DetectionInterval = %v, want 0", c.DetectionInterval)
	}
	if c.WebhookURLs != nil {
		t.Errorf("WebhookURLs = %v, want nil", c.WebhookURLs)
	}
}

func TestDefault_EnvVarsOverride(t *testing.T) {
	t.Setenv("CHRONOS_DB_TYPE", "postgres")
	t.Setenv("CHRONOS_DB_CONN", "postgres://localhost/chronos")
	t.Setenv("CHRONOS_MAX_SIGNALS", "42")
	t.Setenv("CHRONOS_JOB_TIMEOUT", "30s")
	t.Setenv("CHRONOS_SIM_THRESHOLD", "0.9")
	t.Setenv("CHRONOS_HTTP_PORT", "9999")
	t.Setenv("CHRONOS_WEBHOOK_URLS", "https://a.example,https://b.example")
	t.Setenv("CHRONOS_WEBHOOK_RETRIES", "5")
	t.Setenv("CHRONOS_DETECTION_INTERVAL", "1m")

	c := Default()
	if c.DBType != "postgres" {
		t.Errorf("DBType = %q", c.DBType)
	}
	if c.DBConnStr != "postgres://localhost/chronos" {
		t.Errorf("DBConnStr = %q", c.DBConnStr)
	}
	if c.MaxSignalsPerRun != 42 {
		t.Errorf("MaxSignalsPerRun = %d", c.MaxSignalsPerRun)
	}
	if c.ComputationTimeout != 30*time.Second {
		t.Errorf("ComputationTimeout = %v", c.ComputationTimeout)
	}
	if c.SimilarityThreshold != 0.9 {
		t.Errorf("SimilarityThreshold = %v", c.SimilarityThreshold)
	}
	if c.HTTPPort != 9999 {
		t.Errorf("HTTPPort = %d", c.HTTPPort)
	}
	if len(c.WebhookURLs) != 2 || c.WebhookURLs[0] != "https://a.example" || c.WebhookURLs[1] != "https://b.example" {
		t.Errorf("WebhookURLs = %v", c.WebhookURLs)
	}
	if c.WebhookRetries != 5 {
		t.Errorf("WebhookRetries = %d", c.WebhookRetries)
	}
	if c.DetectionInterval != time.Minute {
		t.Errorf("DetectionInterval = %v", c.DetectionInterval)
	}
}

func TestDefault_BadEnvFallsBackSilently(t *testing.T) {
	// Bad values must not panic — they fall back to defaults so a
	// typo can't crash the binary at startup.
	t.Setenv("CHRONOS_MAX_SIGNALS", "not-a-number")
	t.Setenv("CHRONOS_SIM_THRESHOLD", "not-a-float")
	t.Setenv("CHRONOS_JOB_TIMEOUT", "not-a-duration")
	t.Setenv("CHRONOS_WEBHOOK_URLS", "  ,  ,  ") // all-whitespace entries

	c := Default()
	if c.MaxSignalsPerRun != 100 {
		t.Errorf("MaxSignalsPerRun = %d (expected default fallback)", c.MaxSignalsPerRun)
	}
	if c.SimilarityThreshold != 0.85 {
		t.Errorf("SimilarityThreshold = %v (expected default fallback)", c.SimilarityThreshold)
	}
	if c.ComputationTimeout != 10*time.Minute {
		t.Errorf("ComputationTimeout = %v (expected default fallback)", c.ComputationTimeout)
	}
	if c.WebhookURLs != nil {
		t.Errorf("WebhookURLs from whitespace-only input should fall back to nil; got %v", c.WebhookURLs)
	}
}

func TestDefaultEnvSlice_TrimsAndDropsEmpty(t *testing.T) {
	t.Setenv("CHRONOS_WEBHOOK_URLS", " https://a , , https://b ")
	got := defaultEnvSlice("CHRONOS_WEBHOOK_URLS", nil)
	if len(got) != 2 || got[0] != "https://a" || got[1] != "https://b" {
		t.Errorf("defaultEnvSlice = %v", got)
	}
}

func TestValidate_AcceptsDefaults(t *testing.T) {
	if err := Default().Validate(); err != nil {
		t.Fatalf("Default config should be valid: %v", err)
	}
}

func TestValidate_RejectsEachInvariant(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"empty db_type", func(c *Config) { c.DBType = "" }, "db_type"},
		{"empty db_conn", func(c *Config) { c.DBConnStr = "" }, "db_conn"},
		{"sim < 0", func(c *Config) { c.SimilarityThreshold = -0.1 }, "similarity"},
		{"sim > 1", func(c *Config) { c.SimilarityThreshold = 1.1 }, "similarity"},
		{"sample size < 1", func(c *Config) { c.MinSampleSize = 0 }, "sample size"},
		{"spike z < 0", func(c *Config) { c.SpikeZScore = -1 }, "spike z-score"},
		{"drop z < 0", func(c *Config) { c.DropZScore = -1 }, "drop z-score"},
		{"stall stddev < 0", func(c *Config) { c.StallMaxStdDev = -0.1 }, "stall max stddev"},
		{"anomaly sim out of range high", func(c *Config) { c.AnomalyMaxSimilarity = 1.5 }, "anomaly max similarity"},
		{"anomaly sim out of range low", func(c *Config) { c.AnomalyMaxSimilarity = -2 }, "anomaly max similarity"},
		{"seasonality autocorr out of range", func(c *Config) { c.SeasonalityMinAutocorr = -2 }, "seasonality min autocorrelation"},
		{"correlation min > 1", func(c *Config) { c.CorrelationMin = 2 }, "correlation min"},
		{"correlation min < 0", func(c *Config) { c.CorrelationMin = -1 }, "correlation min"},
		{"changepoint shift < 0", func(c *Config) { c.ChangePointMinShift = -1 }, "changepoint min shift"},
		{"outlier cluster z < 0", func(c *Config) { c.OutlierClusterZ = -1 }, "outlier cluster z"},
		{"cross-scope min > 1", func(c *Config) { c.CrossScopeMin = 1.5 }, "cross-scope min"},
		{"confidence established < 0", func(c *Config) { c.ConfidenceClassEstablished = -1 }, "confidence established"},
		{"confidence strong < 0", func(c *Config) { c.ConfidenceClassStrong = -1 }, "confidence strong"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := Default()
			tc.mutate(c)
			err := c.Validate()
			if err == nil {
				t.Fatal("expected validation error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error = %q, want substring %q", err.Error(), tc.want)
			}
		})
	}
}

func TestEnvHelpers(t *testing.T) {
	t.Run("defaultEnv unset", func(t *testing.T) {
		t.Setenv("CHRONOS_TEST_K", "")
		if got := defaultEnv("CHRONOS_TEST_K", "fb"); got != "fb" {
			t.Errorf("got %q", got)
		}
	})
	t.Run("defaultEnv set", func(t *testing.T) {
		t.Setenv("CHRONOS_TEST_K", "v")
		if got := defaultEnv("CHRONOS_TEST_K", "fb"); got != "v" {
			t.Errorf("got %q", got)
		}
	})
	t.Run("defaultEnvInt good", func(t *testing.T) {
		t.Setenv("CHRONOS_TEST_K", "7")
		if got := defaultEnvInt("CHRONOS_TEST_K", 1); got != 7 {
			t.Errorf("got %d", got)
		}
	})
	t.Run("defaultEnvInt bad", func(t *testing.T) {
		t.Setenv("CHRONOS_TEST_K", "abc")
		if got := defaultEnvInt("CHRONOS_TEST_K", 3); got != 3 {
			t.Errorf("got %d", got)
		}
	})
	t.Run("defaultEnvFloat64 good", func(t *testing.T) {
		t.Setenv("CHRONOS_TEST_K", "0.42")
		if got := defaultEnvFloat64("CHRONOS_TEST_K", 0.1); got != 0.42 {
			t.Errorf("got %v", got)
		}
	})
	t.Run("defaultEnvDuration good", func(t *testing.T) {
		t.Setenv("CHRONOS_TEST_K", "250ms")
		if got := defaultEnvDuration("CHRONOS_TEST_K", time.Second); got != 250*time.Millisecond {
			t.Errorf("got %v", got)
		}
	})
}

// The lookback default is load-bearing: it is the only thing standing
// between a fresh deployment and the unbounded history read that
// OOM-killed engines before 0.12.0. It shipped once at 7 days, which
// allocated 1804Mi against a 2Gi limit on a 73-series fleet. Pin it so
// a future widening is a deliberate edit with a failing test, not a
// one-character drift.
func TestDefault_DetectionLookbackIsBoundedAndSane(t *testing.T) {
	c := Default()
	if c.DetectionLookback <= 0 {
		t.Fatalf("DetectionLookback = %v; a non-positive default is an unbounded read", c.DetectionLookback)
	}
	if want := 24 * time.Hour; c.DetectionLookback != want {
		t.Errorf("DetectionLookback = %v, want %v", c.DetectionLookback, want)
	}
	// Enough history for the hungriest detector at a five-minute
	// cadence: CHRONOS_SEASONALITY_MIN_POINTS defaults to well under
	// the 288 points a day of five-minute windows provides.
	if pts := int(c.DetectionLookback / (5 * time.Minute)); pts < 48 {
		t.Errorf("default lookback yields only %d points at a 5m cadence; too few for the detectors", pts)
	}
}
