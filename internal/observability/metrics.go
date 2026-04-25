package observability

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	HTTPRequestTotal *prometheus.CounterVec
	HTTPRequestDur   *prometheus.HistogramVec
	DBQueryDur       *prometheus.HistogramVec
	RedisCacheTotal  *prometheus.CounterVec
	KafkaPublish       *prometheus.CounterVec
	KafkaAsyncEnqueue  *prometheus.CounterVec
	KafkaConsume     *prometheus.CounterVec
	KafkaLagGauge    *prometheus.GaugeVec
	DBPoolConns      *prometheus.GaugeVec
}

func NewMetrics(service string) *Metrics {
	namespace := "ecommerce"

	m := &Metrics{
		HTTPRequestTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: "http",
				Name:      "requests_total",
				Help:      "Total number of HTTP requests.",
			},
			[]string{"service", "route", "method", "status"},
		),
		HTTPRequestDur: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Subsystem: "http",
				Name:      "request_duration_seconds",
				Help:      "HTTP request latency per route.",
				Buckets:   prometheus.DefBuckets,
			},
			[]string{"service", "route", "method", "status"},
		),
		DBQueryDur: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: namespace,
				Subsystem: "db",
				Name:      "query_duration_seconds",
				Help:      "Database query execution latency.",
				Buckets:   prometheus.DefBuckets,
			},
			[]string{"service", "query"},
		),
		RedisCacheTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: "redis",
				Name:      "cache_total",
				Help:      "Total redis cache hit/miss operations.",
			},
			[]string{"service", "operation", "result"},
		),
		KafkaPublish: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: "kafka",
				Name:      "publish_total",
				Help:      "Total kafka publish attempts.",
			},
			[]string{"service", "topic", "status"},
		),
		KafkaAsyncEnqueue: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: "kafka",
				Name:      "async_enqueue_total",
				Help:      "Kafka async publisher enqueue outcomes (accepted vs dropped when queue is full).",
			},
			[]string{"service", "result"},
		),
		KafkaConsume: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: namespace,
				Subsystem: "kafka",
				Name:      "consume_total",
				Help:      "Total kafka consume attempts.",
			},
			[]string{"service", "topic", "status"},
		),
		KafkaLagGauge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: "kafka",
				Name:      "consumer_lag_messages",
				Help:      "Estimated kafka consumer lag in messages.",
			},
			[]string{"service", "topic", "partition", "group"},
		),
		DBPoolConns: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: "db",
				Name:      "pool_connections",
				Help:      "DB pool connections by state (pool=primary|replica).",
			},
			[]string{"service", "pool", "state"},
		),
	}

	prometheus.MustRegister(
		m.HTTPRequestTotal,
		m.HTTPRequestDur,
		m.DBQueryDur,
		m.RedisCacheTotal,
		m.KafkaPublish,
		m.KafkaAsyncEnqueue,
		m.KafkaConsume,
		m.KafkaLagGauge,
		m.DBPoolConns,
	)

	return m
}

