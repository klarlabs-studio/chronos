package client

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// SignalQuery is a fluent builder for /v1/signals. Construct via
// [Client.Signals]. Chain filters, then call List or Get.
type SignalQuery struct {
	c             *Client
	scope         uuid.UUID
	scopes        []uuid.UUID
	series        *uuid.UUID
	pattern       *string
	since, until  *time.Time
	minConfidence *float64
	limit         int
	sinceCursor   string
	ascending     bool
}

// Scope filters by a single scope ID. Required for List unless Scopes
// is set.
func (q *SignalQuery) Scope(id uuid.UUID) *SignalQuery {
	q.scope = id
	return q
}

// Scopes sets a multi-scope allowlist (HTTP scope_in). Cannot be
// combined with Scope.
func (q *SignalQuery) Scopes(ids ...uuid.UUID) *SignalQuery {
	q.scopes = ids
	return q
}

// Series filters to signals about a single entity.
func (q *SignalQuery) Series(id uuid.UUID) *SignalQuery {
	q.series = &id
	return q
}

// Pattern filters to a single PatternType (use the PatternType*
// constants from this package).
func (q *SignalQuery) Pattern(name string) *SignalQuery {
	q.pattern = &name
	return q
}

// Since restricts results to signals detected at or after t.
func (q *SignalQuery) Since(t time.Time) *SignalQuery {
	q.since = &t
	return q
}

// Until restricts results to signals detected before t.
func (q *SignalQuery) Until(t time.Time) *SignalQuery {
	q.until = &t
	return q
}

// MinConfidence drops signals below threshold.
func (q *SignalQuery) MinConfidence(threshold float64) *SignalQuery {
	q.minConfidence = &threshold
	return q
}

// Limit caps the number of returned signals.
func (q *SignalQuery) Limit(n int) *SignalQuery {
	q.limit = n
	return q
}

// SinceCursor resumes a List after the opaque next_cursor token from
// a previous page.
func (q *SignalQuery) SinceCursor(token string) *SignalQuery {
	q.sinceCursor = token
	return q
}

// Ascending pages oldest first from Since, like a SinceCursor query does,
// so a consumer's first poll is bounded before it has a cursor.
func (q *SignalQuery) Ascending() *SignalQuery {
	q.ascending = true
	return q
}

func (q *SignalQuery) queryValues() (url.Values, error) {
	if q.scope == uuid.Nil && len(q.scopes) == 0 {
		return nil, errors.New("chronos client: Scope or Scopes is required")
	}
	if q.scope != uuid.Nil && len(q.scopes) > 0 {
		return nil, errors.New("chronos client: set Scope or Scopes, not both")
	}
	v := url.Values{}
	if q.scope != uuid.Nil {
		v.Set("scope_id", q.scope.String())
	}
	if len(q.scopes) > 0 {
		parts := make([]string, len(q.scopes))
		for i, id := range q.scopes {
			parts[i] = id.String()
		}
		v.Set("scope_in", strings.Join(parts, ","))
	}
	if q.series != nil {
		v.Set("series", q.series.String())
	}
	if q.pattern != nil {
		v.Set("pattern", *q.pattern)
	}
	if q.since != nil {
		v.Set("since", q.since.UTC().Format(time.RFC3339))
	}
	if q.until != nil {
		v.Set("until", q.until.UTC().Format(time.RFC3339))
	}
	if q.minConfidence != nil {
		v.Set("min_confidence", strconv.FormatFloat(*q.minConfidence, 'f', -1, 64))
	}
	if q.limit > 0 {
		v.Set("limit", strconv.Itoa(q.limit))
	}
	if q.sinceCursor != "" {
		v.Set("since_cursor", q.sinceCursor)
	}
	if q.ascending {
		v.Set("order", "asc")
	}
	return v, nil
}

// List runs the query and returns the matching signals.
func (q *SignalQuery) List(ctx context.Context) ([]Signal, error) {
	page, err := q.ListPage(ctx)
	if err != nil {
		return nil, err
	}
	return page.Signals, nil
}

// ListPage runs the query and returns signals plus the opaque
// next_cursor for polling.
func (q *SignalQuery) ListPage(ctx context.Context) (SignalPage, error) {
	v, err := q.queryValues()
	if err != nil {
		return SignalPage{}, err
	}
	var body SignalPage
	if err := q.c.do(ctx, "GET", "/v1/signals?"+v.Encode(), nil, &body); err != nil {
		return SignalPage{}, err
	}
	return body, nil
}

// Stream subscribes to /v1/signals/stream and returns a channel that
// emits each newly-detected signal until ctx is cancelled or the
// server closes the stream. Scope or Scopes is required (the SSE
// endpoint rejects unscoped streams). Pattern is honoured server-side
// when set.
//
// The returned channel is closed by the SDK on ctx cancellation,
// connection drop, or fatal protocol error — `for sig := range ch`
// is safe.
//
// Streaming bypasses the client's Timeout so connections can outlive
// the configured per-request deadline; cancel via ctx instead.
//
// For at-most-once gap recovery, pair the stream with a Since-keyed
// List call: the persisted /v1/signals query is the source of truth,
// the stream is a courtesy. De-duplicate by Signal.ID.
func (q *SignalQuery) Stream(ctx context.Context) (<-chan Signal, error) {
	if q.scope == uuid.Nil && len(q.scopes) == 0 {
		return nil, errors.New("chronos client: Scope or Scopes is required for Stream")
	}
	if q.scope != uuid.Nil && len(q.scopes) > 0 {
		return nil, errors.New("chronos client: set Scope or Scopes, not both")
	}
	v := url.Values{}
	if q.scope != uuid.Nil {
		v.Set("scope_id", q.scope.String())
	} else {
		parts := make([]string, len(q.scopes))
		for i, id := range q.scopes {
			parts[i] = id.String()
		}
		v.Set("scope_in", strings.Join(parts, ","))
	}
	if q.pattern != nil {
		v.Set("pattern", *q.pattern)
	}
	return q.c.openStream(ctx, "/v1/signals/stream?"+v.Encode())
}

// Get returns a single signal by ID.
func (q *SignalQuery) Get(ctx context.Context, id uuid.UUID) (Signal, error) {
	var out Signal
	if err := q.c.do(ctx, "GET", fmt.Sprintf("/v1/signals/%s", id.String()), nil, &out); err != nil {
		return Signal{}, err
	}
	return out, nil
}
