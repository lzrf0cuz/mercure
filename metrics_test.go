package mercure

import (
	"errors"
	"log/slog"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNumberOfRunningSubscribers(t *testing.T) {
	t.Parallel()

	m := NewPrometheusMetrics(nil)

	tms := &TopicMatcherStore{}
	logger := slog.Default()

	s1 := NewLocalSubscriber("", logger, tms)
	s1.setMatchers(stringsToExactMatchers([]string{"topic1", "topic2"}), stringsToExactMatchers(nil))
	m.SubscriberConnected(s1)
	assertGaugeValue(t, 1.0, m.subscribers)

	s2 := NewLocalSubscriber("", logger, tms)
	s2.setMatchers(stringsToExactMatchers([]string{"topic2"}), stringsToExactMatchers(nil))
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

	tms := &TopicMatcherStore{}
	logger := slog.Default()

	s1 := NewLocalSubscriber("", logger, tms)
	s1.setMatchers(stringsToExactMatchers([]string{"topic1", "topic2"}), stringsToExactMatchers(nil))
	m.SubscriberConnected(s1)
	assertCounterValue(t, 1.0, m.subscribersTotal)

	s2 := NewLocalSubscriber("", logger, tms)
	s2.setMatchers(stringsToExactMatchers([]string{"topic2"}), stringsToExactMatchers(nil))
	m.SubscriberConnected(s2)
	assertCounterValue(t, 2.0, m.subscribersTotal)

	m.SubscriberDisconnected(s1)
	m.SubscriberDisconnected(s2)

	assertCounterValue(t, 2.0, m.subscribersTotal)
}

func TestSubscriberDisconnectedWithReason(t *testing.T) {
	t.Parallel()

	m := NewPrometheusMetrics(nil)

	tss := &TopicMatcherStore{}
	logger := slog.Default()

	s1 := NewLocalSubscriber("", logger, tss)
	s1.setMatchers(stringsToExactMatchers([]string{"topic1"}), stringsToExactMatchers(nil))
	m.SubscriberConnected(s1)

	s2 := NewLocalSubscriber("", logger, tss)
	s2.setMatchers(stringsToExactMatchers([]string{"topic1"}), stringsToExactMatchers(nil))
	m.SubscriberConnected(s2)

	// Verify the optional interface is satisfied: SubscribeHandler relies
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

// Two PrometheusMetrics on one registry, as with several hubs in one Caddy config, must share
// the registered CounterVec. Otherwise the second instance increments a collector no scrape
// sees.
func TestSubscriberDisconnectsTotalAdoptsExistingOnSharedRegistry(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	first := NewPrometheusMetrics(reg)
	second := NewPrometheusMetrics(reg)

	// Both instances must share the same registered collector so increments
	// from either path show up in scrapes.
	if first.subscriberDisconnectsTotal != second.subscriberDisconnectsTotal {
		t.Fatal("second NewPrometheusMetrics must adopt the registry's existing subscriber_disconnects_total CounterVec")
	}

	tss := &TopicMatcherStore{}
	logger := slog.Default()
	s := NewLocalSubscriber("", logger, tss)
	s.setMatchers(stringsToExactMatchers([]string{"https://example.com/t"}), stringsToExactMatchers(nil))

	// Increment via the second instance; the count must surface on the shared collector.
	second.SubscriberDisconnectedWithReason(s, DisconnectReasonClientClosed)

	assertCounterVecValue(t, 1.0, first.subscriberDisconnectsTotal, string(DisconnectReasonClientClosed))
}

// Two PrometheusMetrics on one registry, as with several hubs in one Caddy config, must both
// be visible in a scrape of the connected, accepted and published counts.
func TestHubCountsAdoptExistingOnSharedRegistry(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	first := NewPrometheusMetrics(reg)
	second := NewPrometheusMetrics(reg)

	tss := &TopicMatcherStore{}

	for _, m := range []*PrometheusMetrics{first, second} {
		s := NewLocalSubscriber("", slog.Default(), tss)
		s.setMatchers(stringsToExactMatchers([]string{"https://example.com/t"}), stringsToExactMatchers(nil))
		m.SubscriberConnected(s)
		m.UpdatePublished(&Update{})
	}

	families, err := reg.Gather()
	require.NoError(t, err)

	got := map[string]float64{}

	for _, f := range families {
		for _, metric := range f.GetMetric() {
			got[f.GetName()] = metric.GetGauge().GetValue() + metric.GetCounter().GetValue()
		}
	}

	for _, name := range []string{"mercure_subscribers_connected", "mercure_subscribers_total", "mercure_updates_total"} {
		assert.InDelta(t, 2.0, got[name], 0, "%s must count both hubs", name)
	}
}

// stubRegisterer answers every Register with err.
type stubRegisterer struct {
	err error
}

func (r stubRegisterer) Register(prometheus.Collector) error { return r.err }

func (stubRegisterer) MustRegister(...prometheus.Collector) {}

func (stubRegisterer) Unregister(prometheus.Collector) bool { return false }

var errRegisterFailed = errors.New("register failed")

// registerOrAdopt panics on a registration error other than AlreadyRegisteredError, and when
// the registered collector is not of the caller's type.
func TestRegisterOrAdoptPanics(t *testing.T) {
	t.Parallel()

	counterVec := func() *prometheus.CounterVec {
		return prometheus.NewCounterVec(prometheus.CounterOpts{Name: "x_total", Help: "x"}, []string{"l"})
	}

	assert.PanicsWithError(t, errRegisterFailed.Error(), func() {
		registerOrAdopt(stubRegisterer{err: errRegisterFailed}, counterVec(), "x_total")
	})

	mismatch := prometheus.AlreadyRegisteredError{
		ExistingCollector: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: "x", Help: "x"}, []string{"l"}),
		NewCollector:      counterVec(),
	}

	assert.PanicsWithValue(t, "mercure: existing collector for x_total is not *prometheus.CounterVec", func() {
		registerOrAdopt(stubRegisterer{err: mismatch}, counterVec(), "x_total")
	})
}

func TestUpdatePublishFailed(t *testing.T) {
	t.Parallel()

	m := NewPrometheusMetrics(nil)

	// The optional interface must be satisfied: Hub.Publish relies on this
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

// A second PrometheusMetrics on the same registry must adopt the registered CounterVec.
func TestUpdatesFailedTotalAdoptsExistingOnSharedRegistry(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	first := NewPrometheusMetrics(reg)
	second := NewPrometheusMetrics(reg)

	if first.updatesFailedTotal != second.updatesFailedTotal {
		t.Fatal("second NewPrometheusMetrics must adopt the registry's existing mercure_updates_failed_total CounterVec")
	}

	// Increment via the second instance; the count must surface on the shared collector.
	second.UpdatePublishFailed(&Update{}, PublishFailureReasonTimeout)

	assertCounterVecValue(t, 1.0, first.updatesFailedTotal, string(PublishFailureReasonTimeout))
}

// TestSubscriberDisconnectsTotalCoversAllReasons exercises every declared
// DisconnectReason constant and asserts the counter records each.
func TestSubscriberDisconnectsTotalCoversAllReasons(t *testing.T) {
	t.Parallel()

	m := NewPrometheusMetrics(nil)

	tss := &TopicMatcherStore{}
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
		s.setMatchers(stringsToExactMatchers([]string{"https://example.com/t"}), stringsToExactMatchers(nil))
		m.SubscriberConnected(s)
		m.SubscriberDisconnectedWithReason(s, reason)
		assertCounterVecValue(t, 1.0, m.subscriberDisconnectsTotal, string(reason))
	}

	// Gauge should net to zero: every Connected was matched by exactly one
	// Disconnected (via the WithReason path).
	assertGaugeValue(t, 0.0, m.subscribers)
}

func assertCounterVecValue(t *testing.T, v float64, cv *prometheus.CounterVec, labels ...string) {
	t.Helper()

	var metricOut dto.Metric
	if err := cv.WithLabelValues(labels...).Write(&metricOut); err != nil {
		t.Fatal(err)
	}

	assert.InDelta(t, v, metricOut.GetCounter().GetValue(), 0)
}

// TestObserveWriteFlush: PrometheusMetrics implements WriteFlushObserver, and
// each outcome label records its own latency, failures included.
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

// A second PrometheusMetrics on the same registry must adopt the registered HistogramVec.
func TestWriteFlushLatencyAdoptsExistingOnSharedRegistry(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	first := NewPrometheusMetrics(reg)
	second := NewPrometheusMetrics(reg)

	if first.writeFlushLatency != second.writeFlushLatency {
		t.Fatal("second NewPrometheusMetrics must adopt the registry's existing write_flush_seconds HistogramVec")
	}

	// Observe via the second instance; the sample must surface on the shared collector.
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
		Topics: []string{"topic1"},
	})
	m.UpdatePublished(&Update{
		Topics: []string{"topic2"},
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

	// Optional interface: reportRejection relies on this type-assertion succeeding.
	r, ok := any(m).(AuthorizationRejectionReporter)
	if !ok {
		t.Fatal("PrometheusMetrics must implement AuthorizationRejectionReporter")
	}

	r.AuthorizationRejected("groups:Group-Id", ReasonMismatch)
	r.AuthorizationRejected("groups:Group-Id", ReasonMismatch)
	r.AuthorizationRejected("groups:Group-Id", ReasonHeaderAbsent)
	r.AuthorizationRejected("regions:X-Region", ReasonClaimAbsent)
	r.AuthorizationRejected("regions:X-Region", ReasonMalformed)

	// Split by binding and reason; every declared reason is exercised.
	assertCounterVecValue(t, 2.0, m.claimHeaderRejectedTotal, "groups:Group-Id", string(ReasonMismatch))
	assertCounterVecValue(t, 1.0, m.claimHeaderRejectedTotal, "groups:Group-Id", string(ReasonHeaderAbsent))
	assertCounterVecValue(t, 1.0, m.claimHeaderRejectedTotal, "regions:X-Region", string(ReasonClaimAbsent))
	assertCounterVecValue(t, 1.0, m.claimHeaderRejectedTotal, "regions:X-Region", string(ReasonMalformed))
}

// A second PrometheusMetrics on the same registry must adopt the registered CounterVec.
func TestClaimHeaderRejectedTotalAdoptsExistingOnSharedRegistry(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	first := NewPrometheusMetrics(reg)
	second := NewPrometheusMetrics(reg)

	if first.claimHeaderRejectedTotal != second.claimHeaderRejectedTotal {
		t.Fatal("second NewPrometheusMetrics must adopt the registry's existing mercure_claim_header_rejected_total CounterVec")
	}

	second.AuthorizationRejected("groups:Group-Id", ReasonMismatch)

	assertCounterVecValue(t, 1.0, first.claimHeaderRejectedTotal, "groups:Group-Id", string(ReasonMismatch))
}

// The reporter is an opt-in extension: the base Metrics interface stays minimal, so a
// NopMetrics hub skips reporting rather than failing.
func TestNopMetricsDoesNotReportAuthorizationRejections(t *testing.T) {
	t.Parallel()

	_, ok := any(NopMetrics{}).(AuthorizationRejectionReporter)
	assert.False(t, ok)
}

// The claim-value counter has its own name and a claim label, and each reason increments
// only its own series.
func TestClaimValueRejected(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewPedanticRegistry()
	m := NewPrometheusMetrics(registry)

	r, ok := any(m).(ClaimValueRejectionReporter)
	if !ok {
		t.Fatal("PrometheusMetrics must implement ClaimValueRejectionReporter")
	}

	r.ClaimValueRejected("groups", ReasonMismatch)
	r.ClaimValueRejected("groups", ReasonMismatch)
	r.ClaimValueRejected("groups", ReasonClaimAbsent)
	r.ClaimValueRejected("regions", ReasonMalformed)

	assertCounterVecValue(t, 2.0, m.claimValueRejectedTotal, "groups", string(ReasonMismatch))
	assertCounterVecValue(t, 1.0, m.claimValueRejectedTotal, "groups", string(ReasonClaimAbsent))
	assertCounterVecValue(t, 1.0, m.claimValueRejectedTotal, "regions", string(ReasonMalformed))
	assertCounterVecValue(t, 0.0, m.claimValueRejectedTotal, "groups", string(ReasonMalformed))

	families, err := registry.Gather()
	require.NoError(t, err)

	var found bool

	for _, f := range families {
		switch f.GetName() {
		case "mercure_claim_header_rejected_total":
			t.Fatal("a claim-value rejection must not create a claim-header series")
		case "mercure_claim_value_rejected_total":
			found = true

			assert.Equal(t, dto.MetricType_COUNTER, f.GetType())

			for _, metric := range f.GetMetric() {
				names := make([]string, 0, len(metric.GetLabel()))
				for _, l := range metric.GetLabel() {
					names = append(names, l.GetName())
				}

				assert.Equal(t, []string{"claim", "reason"}, names)
			}
		}
	}

	assert.True(t, found, "mercure_claim_value_rejected_total must be registered")
}

// A second PrometheusMetrics on the same registry must adopt the registered CounterVec.
func TestClaimValueRejectedTotalAdoptsExistingOnSharedRegistry(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	first := NewPrometheusMetrics(reg)
	second := NewPrometheusMetrics(reg)

	if first.claimValueRejectedTotal != second.claimValueRejectedTotal {
		t.Fatal("second NewPrometheusMetrics must adopt the registry's existing mercure_claim_value_rejected_total CounterVec")
	}

	second.ClaimValueRejected("groups", ReasonMismatch)

	assertCounterVecValue(t, 1.0, first.claimValueRejectedTotal, "groups", string(ReasonMismatch))
}

func TestNopMetricsDoesNotReportClaimValueRejections(t *testing.T) {
	t.Parallel()

	_, ok := any(NopMetrics{}).(ClaimValueRejectionReporter)
	assert.False(t, ok)
}

// testBindingID is the binding identity of the counting binding in the metrics tests.
const testBindingID = "groups:Group-Id"

// boundSubscriber returns a subscriber counted under testBindingID and value, or not counted
// when value is "".
func boundSubscriber(value string) *LocalSubscriber {
	s := NewLocalSubscriber("", slog.Default(), &TopicMatcherStore{})
	s.counted = bindingValue{binding: testBindingID, value: value}

	return s
}

// gatherBindingValues returns the mercure_subscribers_by_binding_value series a scrape of g would
// see, keyed by the value label. Every series must carry the binding label testBindingID.
func gatherBindingValues(tb testing.TB, g prometheus.Gatherer) map[string]float64 {
	tb.Helper()

	families, err := g.Gather()
	require.NoError(tb, err)

	got := map[string]float64{}

	for _, f := range families {
		if f.GetName() != "mercure_subscribers_by_binding_value" {
			continue
		}

		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}

			require.Equal(tb, map[string]string{"binding": testBindingID, "value": labels["value"]}, labels)
			got[labels["value"]] = m.GetGauge().GetValue()
		}
	}

	return got
}

// snapshot returns the counts keyed by value, failing the test on a binding other than testBindingID.
func (b *bindingValueSubscribers) snapshot(tb testing.TB) map[string]int {
	tb.Helper()

	b.mu.Lock()
	defer b.mu.Unlock()

	got := make(map[string]int, len(b.counts))

	for bv, n := range b.counts {
		require.Equal(tb, testBindingID, bv.binding)

		got[bv.value] = n
	}

	return got
}

func TestSubscribersByBindingValue(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewPedanticRegistry()
	m := NewPrometheusMetrics(reg)

	assert.Empty(t, gatherBindingValues(t, reg), "no series until a subscriber is counted")

	a1, a2, b1 := boundSubscriber("red"), boundSubscriber("red"), boundSubscriber("blue")
	anonymous := boundSubscriber("")

	m.SubscriberConnected(a1)
	m.SubscriberConnected(a2)
	m.SubscriberConnected(b1)
	m.SubscriberConnected(anonymous)
	assert.Equal(t, map[string]float64{"red": 2, "blue": 1}, gatherBindingValues(t, reg))

	m.SubscriberDisconnected(anonymous)
	m.SubscriberDisconnected(a1)
	assert.Equal(t, map[string]float64{"red": 1, "blue": 1}, gatherBindingValues(t, reg))

	m.SubscriberDisconnectedWithReason(a2, DisconnectReasonClientClosed)
	assert.Equal(t, map[string]float64{"blue": 1}, gatherBindingValues(t, reg))

	// A disconnect for a value with no count changes nothing and never goes negative.
	m.SubscriberDisconnected(a2)
	m.SubscriberDisconnectedWithReason(boundSubscriber("green"), DisconnectReasonClientClosed)
	assert.Equal(t, map[string]float64{"blue": 1}, gatherBindingValues(t, reg))
	assert.Equal(t, map[string]int{"blue": 1}, m.subscribersByBindingValue.snapshot(t))

	m.SubscriberDisconnectedWithReason(b1, DisconnectReasonHubShutdown)
	assert.Empty(t, gatherBindingValues(t, reg))
	assert.Empty(t, m.subscribersByBindingValue.snapshot(t))
}

// gatherBindingSeries returns every mercure_subscribers_by_binding_value series a scrape of g would
// see, keyed by "binding value".
func gatherBindingSeries(tb testing.TB, g prometheus.Gatherer) map[string]float64 {
	tb.Helper()

	families, err := g.Gather()
	require.NoError(tb, err)

	got := map[string]float64{}

	for _, f := range families {
		if f.GetName() != "mercure_subscribers_by_binding_value" {
			continue
		}

		for _, m := range f.GetMetric() {
			labels := map[string]string{}
			for _, l := range m.GetLabel() {
				labels[l.GetName()] = l.GetValue()
			}

			got[labels["binding"]+" "+labels["value"]] = m.GetGauge().GetValue()
		}
	}

	return got
}

// Two bindings can match the same header value, so the count key includes the binding: each pair
// is its own series, counted and deleted independently.
func TestSubscribersByBindingValueSeparatesBindings(t *testing.T) {
	t.Parallel()

	const (
		groups  = "groups:Group-Id"
		regions = "regions:X-Region"
	)

	reg := prometheus.NewPedanticRegistry()
	first := NewPrometheusMetrics(reg)
	second := NewPrometheusMetrics(reg)

	counted := func(binding string) *LocalSubscriber {
		s := boundSubscriber("red")
		s.counted.binding = binding

		return s
	}

	g1, r1, r2 := counted(groups), counted(regions), counted(regions)

	first.SubscriberConnected(g1)
	first.SubscriberConnected(r1)
	second.SubscriberConnected(r2)
	assert.Equal(t, map[string]float64{groups + " red": 1, regions + " red": 2}, gatherBindingSeries(t, reg))

	first.SubscriberDisconnected(r1)
	assert.Equal(t, map[string]float64{groups + " red": 1, regions + " red": 1}, gatherBindingSeries(t, reg))

	second.SubscriberDisconnectedWithReason(g1, DisconnectReasonClientClosed)
	assert.Equal(t, map[string]float64{regions + " red": 1}, gatherBindingSeries(t, reg))

	// A repeat disconnect of the deleted series is a no-op and leaves the other binding alone.
	first.SubscriberDisconnected(g1)
	assert.Equal(t, map[string]float64{regions + " red": 1}, gatherBindingSeries(t, reg))

	first.SubscriberDisconnected(r2)
	assert.Empty(t, gatherBindingSeries(t, reg))
}

func TestSubscribersByBindingValueConcurrent(t *testing.T) {
	t.Parallel()

	const (
		workers    = 30
		iterations = 200
	)

	reg := prometheus.NewRegistry()
	m := NewPrometheusMetrics(reg)
	groups := []string{"red", "blue", "green"}

	var wg sync.WaitGroup

	for w := range workers {
		wg.Go(func() {
			s := boundSubscriber(groups[w%len(groups)])
			anonymous := boundSubscriber("")

			for i := range iterations {
				m.SubscriberConnected(s)
				m.SubscriberConnected(anonymous)

				if i%2 == 0 {
					m.SubscriberDisconnected(s)
				} else {
					m.SubscriberDisconnectedWithReason(s, DisconnectReasonClientClosed)
				}

				m.SubscriberDisconnected(anonymous)
			}

			// Worker w leaves w%3 subscribers connected: none for red, one for blue and
			// two for green.
			for range w % len(groups) {
				m.SubscriberConnected(s)
			}
		})
	}

	wg.Wait()

	const perValue = workers / 3

	assert.Equal(t, map[string]float64{"blue": perValue, "green": 2 * perValue}, gatherBindingValues(t, reg))
	assert.Equal(t, map[string]int{"blue": perValue, "green": 2 * perValue}, m.subscribersByBindingValue.snapshot(t))
}

// A connect racing the last disconnect of the same value ends at one subscriber whichever
// runs first: 1 → 0 (deleted) → 1, or 1 → 2 → 1.
func TestSubscribersByBindingValueConnectRacesLastDisconnect(t *testing.T) {
	t.Parallel()

	ts := newBindingValueSubscribers()
	reg := prometheus.NewRegistry()
	reg.MustRegister(ts)

	ts.connected(bindingValue{binding: testBindingID, value: "red"})

	for range 10000 {
		// Both goroutines spin until the other is running, so the two calls overlap.
		var ready atomic.Int32

		arrive := func() {
			ready.Add(1)

			for ready.Load() < 2 {
				runtime.Gosched()
			}
		}

		var wg sync.WaitGroup

		wg.Go(func() {
			arrive()
			ts.disconnected(bindingValue{binding: testBindingID, value: "red"})
		})
		wg.Go(func() {
			arrive()
			ts.connected(bindingValue{binding: testBindingID, value: "red"})
		})

		wg.Wait()

		require.Equal(t, map[string]float64{"red": 1}, gatherBindingValues(t, reg))
		require.Equal(t, map[string]int{"red": 1}, ts.snapshot(t))
	}
}

// Two PrometheusMetrics on one registry share the binding-value counts, so a subscriber connected
// through one instance is decremented through the other.
func TestSubscribersByBindingValueAdoptsExistingOnSharedRegistry(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewRegistry()
	first := NewPrometheusMetrics(reg)
	second := NewPrometheusMetrics(reg)

	require.Same(t, first.subscribersByBindingValue, second.subscribersByBindingValue)

	s1, s2 := boundSubscriber("red"), boundSubscriber("red")

	first.SubscriberConnected(s1)
	first.SubscriberConnected(s2)
	assert.Equal(t, map[string]float64{"red": 2}, gatherBindingValues(t, reg))

	second.SubscriberDisconnectedWithReason(s1, DisconnectReasonHubShutdown)
	assert.Equal(t, map[string]float64{"red": 1}, gatherBindingValues(t, reg))

	second.SubscriberDisconnected(s2)
	assert.Empty(t, gatherBindingValues(t, reg))

	// A repeated disconnect through either instance stays a no-op.
	first.SubscriberDisconnected(s2)
	second.SubscriberDisconnected(s2)
	assert.Empty(t, gatherBindingValues(t, reg))
	assert.Empty(t, first.subscribersByBindingValue.snapshot(t))
}

// TestMetricContract pins what a scrape and every dashboard or alert depends on for each hub
// collector: the fully qualified name, help text, label names and, for the histogram, the
// buckets. Renaming a metric or label or rewording its help is a breaking change to operators.
func TestMetricContract(t *testing.T) {
	t.Parallel()

	reg := prometheus.NewPedanticRegistry()
	m := NewPrometheusMetrics(reg)

	// A vec exposes no series until it has a child, so give each one a sample.
	m.SubscriberConnected(boundSubscriber("red"))
	m.SubscriberDisconnectedWithReason(boundSubscriber(""), DisconnectReasonClientClosed)
	m.UpdatePublishFailed(&Update{}, PublishFailureReasonValidation)
	m.AuthorizationRejected("groups:Group-Id", ReasonMismatch)
	m.ClaimValueRejected("groups", ReasonMismatch)
	m.ObserveWriteFlush(0.001, true)
	m.UpdatePublished(&Update{})

	families, err := reg.Gather()
	require.NoError(t, err)

	byName := map[string]*dto.MetricFamily{}
	for _, f := range families {
		byName[f.GetName()] = f
	}

	tests := []struct {
		name    string
		typ     dto.MetricType
		help    string
		labels  []string
		buckets []float64
	}{
		{
			name:   "mercure_subscribers_connected",
			typ:    dto.MetricType_GAUGE,
			help:   "Number of subscribers currently connected to the hub (instantaneous).",
			labels: []string{},
		},
		{
			name:   "mercure_subscribers_total",
			typ:    dto.MetricType_COUNTER,
			help:   "Total number of subscribers accepted by the hub since startup (cumulative; includes those that have since disconnected). Pair with mercure_subscribers_connected to compute churn.",
			labels: []string{},
		},
		{
			name:   "mercure_updates_total",
			typ:    dto.MetricType_COUNTER,
			help:   "Total number of updates accepted by the hub for dispatch since startup (cumulative; counts publish-time, not per-subscriber delivery).",
			labels: []string{},
		},
		{
			name:   "mercure_subscriber_disconnects_total",
			typ:    dto.MetricType_COUNTER,
			help:   "Subscriber disconnections labeled by reason (client_closed | hub_shutdown | write_timeout | write_failed | transport_ended | transport_error | unknown). Pair with mercure_subscribers_connected for cause-tagged churn analysis.",
			labels: []string{"reason"},
		},
		{
			name:    "mercure_subscribe_write_flush_seconds",
			typ:     dto.MetricType_HISTOGRAM,
			help:    "Time spent in the per-event SSE write+flush path (set deadline → Write → Flush → reset deadline) labeled by outcome (ok|failed). Failed writes also record their latency so a slow broken-pipe stays distinguishable from a slow healthy write. Tail latency here pairs with mercure_redis_xreadgroup_latency_seconds to localize hub-side vs transport-side stalls.",
			labels:  []string{"outcome"},
			buckets: []float64{0.0001, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1, 2.5, 5, 10},
		},
		{
			name:   "mercure_updates_failed_total",
			typ:    dto.MetricType_COUNTER,
			help:   "Updates that failed to publish, labeled by reason (validation | transport | timeout). Counts validation + dispatch failures only. Auth rejections and unreadable or oversized request bodies are not included; an update whose encoding exceeds the transport's cap counts as validation. Pair with mercure_updates_total for a success/failure ratio.",
			labels: []string{"reason"},
		},
		{
			name:   "mercure_claim_header_rejected_total",
			typ:    dto.MetricType_COUNTER,
			help:   "Requests rejected by a claim-header authorization binding (require_claim_header), labeled by binding (claim:Canonical-Header, e.g. groups:Group-Id) and reason (header_absent | claim_absent | mismatch | malformed). header_absent answers 400, claim_absent and mismatch 403; malformed answers 400 for the bound header and 401 for the bound claim.",
			labels: []string{"binding", "reason"},
		},
		{
			name:   "mercure_claim_value_rejected_total",
			typ:    dto.MetricType_COUNTER,
			help:   "Requests rejected by a claim-value binding (require_claim_value), labeled by claim and reason (claim_absent | mismatch | malformed). claim_absent and mismatch answer 403, malformed 401.",
			labels: []string{"claim", "reason"},
		},
		{
			name:   "mercure_subscribers_by_binding_value",
			typ:    dto.MetricType_GAUGE,
			help:   "Connected subscribers per binding value, labeled by binding (claim:Canonical-Header, as in mercure_claim_header_rejected_total) and value (the header value matched by the binding that counts subscribers).",
			labels: []string{"binding", "value"},
		},
		{
			name:   "mercure_version_info",
			typ:    dto.MetricType_GAUGE,
			help:   "A metric with a constant '1' value labeled by different build stats fields.",
			labels: []string{"architecture", "built_at", "commit", "go_version", "os", "upstream_version", "version"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f, ok := byName[tt.name]
			require.True(t, ok, "%s must be exposed", tt.name)
			require.NotEmpty(t, f.GetMetric())

			assert.Equal(t, tt.typ, f.GetType())
			assert.Equal(t, tt.help, f.GetHelp())

			labels := make([]string, 0, len(f.GetMetric()[0].GetLabel()))
			for _, l := range f.GetMetric()[0].GetLabel() {
				labels = append(labels, l.GetName())
			}

			assert.ElementsMatch(t, tt.labels, labels)

			if tt.typ != dto.MetricType_HISTOGRAM {
				return
			}

			var buckets []float64

			for _, b := range f.GetMetric()[0].GetHistogram().GetBucket() {
				buckets = append(buckets, b.GetUpperBound())
			}

			assert.Equal(t, tt.buckets, buckets)
		})
	}
}
