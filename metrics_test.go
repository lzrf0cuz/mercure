package mercure

import (
	"log/slog"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
)

func TestNumberOfRunningSubscribers(t *testing.T) {
	t.Parallel()

	m := NewPrometheusMetrics(nil)

	tss := &TopicSelectorStore{}
	logger := slog.Default()

	s1 := NewLocalSubscriber("", logger, tss)
	s1.SetTopics([]string{"topic1", "topic2"}, nil)
	m.SubscriberConnected(s1)
	assertGaugeValue(t, 1.0, m.subscribers)

	s2 := NewLocalSubscriber("", logger, tss)
	s2.SetTopics([]string{"topic2"}, nil)
	m.SubscriberConnected(s2)
	assertGaugeValue(t, 2.0, m.subscribers)

	m.SubscriberDisconnected(s1)
	assertGaugeValue(t, 1.0, m.subscribers)

	m.SubscriberDisconnected(s2)
	assertGaugeValue(t, 0.0, m.subscribers)
}

func TestTotalNumberOfHandledSubscribers(t *testing.T) {
	t.Parallel()

	m := NewPrometheusMetrics(nil)

	tss := &TopicSelectorStore{}
	logger := slog.Default()

	s1 := NewLocalSubscriber("", logger, tss)
	s1.SetTopics([]string{"topic1", "topic2"}, nil)
	m.SubscriberConnected(s1)
	assertCounterValue(t, 1.0, m.subscribersTotal)

	s2 := NewLocalSubscriber("", logger, tss)
	s2.SetTopics([]string{"topic2"}, nil)
	m.SubscriberConnected(s2)
	assertCounterValue(t, 2.0, m.subscribersTotal)

	m.SubscriberDisconnected(s1)
	m.SubscriberDisconnected(s2)

	assertCounterValue(t, 2.0, m.subscribersTotal)
}

func TestSubscriberDisconnectedWithReason(t *testing.T) {
	t.Parallel()

	m := NewPrometheusMetrics(nil)

	tss := &TopicSelectorStore{}
	logger := slog.Default()

	s1 := NewLocalSubscriber("", logger, tss)
	s1.SetTopics([]string{"topic1"}, nil)
	m.SubscriberConnected(s1)

	s2 := NewLocalSubscriber("", logger, tss)
	s2.SetTopics([]string{"topic1"}, nil)
	m.SubscriberConnected(s2)

	// Verify the optional interface is satisfied — SubscribeHandler relies
	// on this type-assertion succeeding for PrometheusMetrics.
	r, ok := any(m).(DisconnectReasonReporter)
	if !ok {
		t.Fatal("PrometheusMetrics must implement DisconnectReasonReporter")
	}

	r.SubscriberDisconnectedWithReason(s1, DisconnectReasonClientClosed)
	r.SubscriberDisconnectedWithReason(s2, DisconnectReasonHubShutdown)

	// Gauge dec'd by both calls.
	assertGaugeValue(t, 0.0, m.subscribers)

	// Per-reason counters split correctly.
	assertCounterVecValue(t, 1.0, m.subscriberDisconnectsTotal, string(DisconnectReasonClientClosed))
	assertCounterVecValue(t, 1.0, m.subscriberDisconnectsTotal, string(DisconnectReasonHubShutdown))
	assertCounterVecValue(t, 0.0, m.subscriberDisconnectsTotal, string(DisconnectReasonWriteTimeout))
}

// TestSubscriberDisconnectsTotalAdoptsExistingOnReload verifies that when
// NewPrometheusMetrics is called twice against the same registry — the Caddy
// config-reload scenario — the second instance adopts the registry's existing
// CounterVec rather than retaining a freshly-allocated, unregistered one.
// Without this adoption, post-reload SubscriberDisconnectedWithReason calls
// would increment a detached collector that scrapes never see.
func TestSubscriberDisconnectsTotalAdoptsExistingOnReload(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	first := NewPrometheusMetrics(reg)
	second := NewPrometheusMetrics(reg)

	// Both instances must share the same registered collector so increments
	// from either path show up in scrapes.
	if first.subscriberDisconnectsTotal != second.subscriberDisconnectsTotal {
		t.Fatal("second NewPrometheusMetrics must adopt the registry's existing subscriber_disconnects_total CounterVec")
	}

	tss := &TopicSelectorStore{}
	logger := slog.Default()
	s := NewLocalSubscriber("", logger, tss)
	s.SetTopics([]string{"https://example.com/t"}, nil)

	// Increment via the second (post-reload) instance — the count must
	// surface on the registry that the first instance's scrape would see.
	second.SubscriberDisconnectedWithReason(s, DisconnectReasonClientClosed)

	assertCounterVecValue(t, 1.0, first.subscriberDisconnectsTotal, string(DisconnectReasonClientClosed))
}

func TestUpdatePublishFailed(t *testing.T) {
	t.Parallel()

	m := NewPrometheusMetrics(nil)

	// The optional interface must be satisfied — Hub.Publish relies on this
	// type-assertion succeeding for PrometheusMetrics.
	r, ok := any(m).(PublishFailureReporter)
	if !ok {
		t.Fatal("PrometheusMetrics must implement PublishFailureReporter")
	}

	r.UpdatePublishFailed(&Update{}, PublishFailureReasonValidation)
	r.UpdatePublishFailed(&Update{}, PublishFailureReasonTransport)
	r.UpdatePublishFailed(&Update{}, PublishFailureReasonTransport)
	r.UpdatePublishFailed(&Update{}, PublishFailureReasonTimeout)

	// Per-reason counters split correctly; every declared reason is exercised.
	assertCounterVecValue(t, 1.0, m.updatesFailedTotal, string(PublishFailureReasonValidation))
	assertCounterVecValue(t, 2.0, m.updatesFailedTotal, string(PublishFailureReasonTransport))
	assertCounterVecValue(t, 1.0, m.updatesFailedTotal, string(PublishFailureReasonTimeout))
}

// TestUpdatesFailedTotalAdoptsExistingOnReload mirrors the disconnect-counter
// reload test: a second NewPrometheusMetrics against the same registry (the
// Caddy config-reload scenario) must adopt the existing CounterVec, or
// post-reload UpdatePublishFailed calls would increment a detached collector.
func TestUpdatesFailedTotalAdoptsExistingOnReload(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	first := NewPrometheusMetrics(reg)
	second := NewPrometheusMetrics(reg)

	if first.updatesFailedTotal != second.updatesFailedTotal {
		t.Fatal("second NewPrometheusMetrics must adopt the registry's existing mercure_updates_failed_total CounterVec")
	}

	// Increment via the post-reload instance — the count must surface on the
	// registry the first instance's scrape would see.
	second.UpdatePublishFailed(&Update{}, PublishFailureReasonTimeout)

	assertCounterVecValue(t, 1.0, first.updatesFailedTotal, string(PublishFailureReasonTimeout))
}

// TestSubscriberDisconnectsTotalCoversAllReasons exhaustively exercises every
// declared DisconnectReason constant and asserts the counter records each.
// Catches future taxonomy additions that forget to ship a corresponding test
// (the closed-set invariant is enforced by review + this exhaustive check).
func TestSubscriberDisconnectsTotalCoversAllReasons(t *testing.T) {
	t.Parallel()

	m := NewPrometheusMetrics(nil)

	tss := &TopicSelectorStore{}
	logger := slog.Default()

	reasons := []DisconnectReason{
		DisconnectReasonClientClosed,
		DisconnectReasonHubShutdown,
		DisconnectReasonWriteTimeout,
		DisconnectReasonWriteFailed,
		DisconnectReasonTransportEnded,
		DisconnectReasonTransportError,
		DisconnectReasonUnknown,
	}

	for _, reason := range reasons {
		s := NewLocalSubscriber("", logger, tss)
		s.SetTopics([]string{"https://example.com/t"}, nil)
		m.SubscriberConnected(s)
		m.SubscriberDisconnectedWithReason(s, reason)
		assertCounterVecValue(t, 1.0, m.subscriberDisconnectsTotal, string(reason))
	}

	// Gauge should net to zero — every Connected was matched by exactly one
	// Disconnected (via the WithReason path).
	assertGaugeValue(t, 0.0, m.subscribers)
}

func assertCounterVecValue(t *testing.T, v float64, cv *prometheus.CounterVec, labels ...string) {
	t.Helper()

	var metricOut dto.Metric
	if err := cv.WithLabelValues(labels...).Write(&metricOut); err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, v, metricOut.GetCounter().GetValue()) //nolint:testifylint
}

// TestObserveWriteFlush verifies the optional WriteFlushObserver interface
// is satisfied and both outcome labels record their own latency. Failed
// observations must be retained (not skipped) so the histogram exposes the
// error-path latency distribution — that's what makes the metric useful for
// distinguishing "broken pipe in 200µs" from "deadline exceeded near dispatchTimeout".
func TestObserveWriteFlush(t *testing.T) {
	t.Parallel()

	m := NewPrometheusMetrics(nil)

	o, ok := any(m).(WriteFlushObserver)
	if !ok {
		t.Fatal("PrometheusMetrics must implement WriteFlushObserver")
	}

	o.ObserveWriteFlush(0.0007, true)
	o.ObserveWriteFlush(0.0012, true)
	o.ObserveWriteFlush(30.0, false)

	assertHistogramVecCount(t, 2, m.writeFlushLatency, "ok")
	assertHistogramVecCount(t, 1, m.writeFlushLatency, "failed")
}

// TestWriteFlushLatencyAdoptsExistingOnReload locks the same Caddy-reload
// adoption pattern proved out for subscriberDisconnectsTotal: a second
// NewPrometheusMetrics against the same registry must reuse the registry's
// existing HistogramVec, not retain a freshly-allocated detached one.
// Without adoption, post-reload ObserveWriteFlush calls would update a
// HistogramVec that scrapes never see.
func TestWriteFlushLatencyAdoptsExistingOnReload(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	first := NewPrometheusMetrics(reg)
	second := NewPrometheusMetrics(reg)

	if first.writeFlushLatency != second.writeFlushLatency {
		t.Fatal("second NewPrometheusMetrics must adopt the registry's existing write_flush_seconds HistogramVec")
	}

	// Increment via the post-reload instance — sample must surface on the
	// shared registered collector.
	second.ObserveWriteFlush(0.0005, true)

	assertHistogramVecCount(t, 1, first.writeFlushLatency, "ok")
}

func assertHistogramVecCount(t *testing.T, want uint64, hv *prometheus.HistogramVec, labels ...string) {
	t.Helper()

	obs, err := hv.GetMetricWithLabelValues(labels...)
	if err != nil {
		t.Fatal(err)
	}

	var metricOut dto.Metric
	if err := obs.(prometheus.Histogram).Write(&metricOut); err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, want, metricOut.GetHistogram().GetSampleCount())
}

func TestTotalOfHandledUpdates(t *testing.T) {
	t.Parallel()

	m := NewPrometheusMetrics(nil)

	m.UpdatePublished(&Update{
		Topics: []string{"topic1", "topic2"},
	})
	m.UpdatePublished(&Update{
		Topics: []string{"topic2", "topic3"},
	})
	m.UpdatePublished(&Update{
		Topics: []string{"topic2"},
	})
	m.UpdatePublished(&Update{
		Topics: []string{"topic3"},
	})

	assertCounterValue(t, 4.0, m.updatesTotal)
}

func assertGaugeValue(t *testing.T, v float64, g prometheus.Gauge) {
	t.Helper()

	var metricOut dto.Metric
	if err := g.Write(&metricOut); err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, v, metricOut.GetGauge().GetValue()) //nolint:testifylint
}

func assertCounterValue(t *testing.T, v float64, c prometheus.Counter) {
	t.Helper()

	var metricOut dto.Metric
	if err := c.Write(&metricOut); err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, v, metricOut.GetCounter().GetValue()) // nolint:testifylint
}

// The claim-header rejection counter is binding-specific: a broad "authorization_rejected"
// would imply the hub meters every auth failure, which it does not (mercure_updates_failed_total
// explicitly excludes HTTP-layer auth rejections).
func TestAuthorizationRejected(t *testing.T) {
	t.Parallel()

	m := NewPrometheusMetrics(nil)

	// Optional interface: authorizeAndBind relies on this type-assertion succeeding.
	r, ok := any(m).(AuthorizationRejectionReporter)
	if !ok {
		t.Fatal("PrometheusMetrics must implement AuthorizationRejectionReporter")
	}

	r.AuthorizationRejected("tenants:Tenant-Id", ReasonMismatch)
	r.AuthorizationRejected("tenants:Tenant-Id", ReasonMismatch)
	r.AuthorizationRejected("tenants:Tenant-Id", ReasonHeaderAbsent)
	r.AuthorizationRejected("regions:X-Region", ReasonClaimAbsent)
	r.AuthorizationRejected("regions:X-Region", ReasonMalformed)

	// Split by binding AND reason; every declared reason is exercised.
	assertCounterVecValue(t, 2.0, m.claimHeaderRejectedTotal, "tenants:Tenant-Id", string(ReasonMismatch))
	assertCounterVecValue(t, 1.0, m.claimHeaderRejectedTotal, "tenants:Tenant-Id", string(ReasonHeaderAbsent))
	assertCounterVecValue(t, 1.0, m.claimHeaderRejectedTotal, "regions:X-Region", string(ReasonClaimAbsent))
	assertCounterVecValue(t, 1.0, m.claimHeaderRejectedTotal, "regions:X-Region", string(ReasonMalformed))
}

// Caddy reuses a Prometheus registry across config reloads; a detached CounterVec would
// silently swallow every post-reload increment.
func TestClaimHeaderRejectedTotalAdoptsExistingOnReload(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	first := NewPrometheusMetrics(reg)
	second := NewPrometheusMetrics(reg)

	if first.claimHeaderRejectedTotal != second.claimHeaderRejectedTotal {
		t.Fatal("second NewPrometheusMetrics must adopt the registry's existing mercure_claim_header_rejected_total CounterVec")
	}

	second.AuthorizationRejected("tenants:Tenant-Id", ReasonMismatch)

	assertCounterVecValue(t, 1.0, first.claimHeaderRejectedTotal, "tenants:Tenant-Id", string(ReasonMismatch))
}

// The reporter is an opt-in extension: the base Metrics interface stays minimal, so a
// NopMetrics hub skips reporting rather than failing.
func TestNopMetricsDoesNotReportAuthorizationRejections(t *testing.T) {
	t.Parallel()

	_, ok := any(NopMetrics{}).(AuthorizationRejectionReporter)
	assert.False(t, ok)
}
