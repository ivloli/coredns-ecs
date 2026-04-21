package ecs_normalizer

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/miekg/dns"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	metricsRegisterOnce sync.Once

	ecsRequestTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "coredns",
			Subsystem: "ecs_normalizer",
			Name:      "requests_total",
			Help:      "Total DNS requests processed by ecs_normalizer.",
		},
		[]string{"rcode", "cache_hit"},
	)

	ecsRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "coredns",
			Subsystem: "ecs_normalizer",
			Name:      "request_duration_seconds",
			Help:      "End-to-end DNS request duration in ecs_normalizer.",
			Buckets:   prometheus.ExponentialBuckets(0.001, 2, 15),
		},
		[]string{"rcode", "cache_hit"},
	)

	ecsSuccessTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "coredns",
			Subsystem: "ecs_normalizer",
			Name:      "success_total",
			Help:      "Total successful DNS responses (rcode=NOERROR).",
		},
	)

	ecsNXDomainTotal = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "coredns",
			Subsystem: "ecs_normalizer",
			Name:      "nxdomain_total",
			Help:      "Total NXDOMAIN responses.",
		},
	)
)

func registerMetrics() {
	metricsRegisterOnce.Do(func() {
		prometheus.MustRegister(ecsRequestTotal)
		prometheus.MustRegister(ecsRequestDuration)
		prometheus.MustRegister(ecsSuccessTotal)
		prometheus.MustRegister(ecsNXDomainTotal)
	})
}

func (e *ECSNormalizer) observeRequestMetrics(elapsed time.Duration, rcode int, err error, cacheHit bool) {
	rcodeLabel := formatRcodeLabel(rcode)
	cacheHitLabel := strconv.FormatBool(cacheHit)

	ecsRequestTotal.WithLabelValues(rcodeLabel, cacheHitLabel).Inc()
	ecsRequestDuration.WithLabelValues(rcodeLabel, cacheHitLabel).Observe(elapsed.Seconds())

	if err == nil {
		if rcode == dns.RcodeSuccess {
			ecsSuccessTotal.Inc()
		}
		if rcode == dns.RcodeNameError {
			ecsNXDomainTotal.Inc()
		}
	}
}

func formatRcodeLabel(rcode int) string {
	if s, ok := dns.RcodeToString[rcode]; ok {
		return strings.ToUpper(s)
	}
	return strconv.Itoa(rcode)
}
