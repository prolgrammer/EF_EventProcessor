package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics holds all Prometheus metrics for the event-processor service.
// Following the RED (Rate / Errors / Duration) method.
type Metrics struct {
	// Pipeline throughput
	EventsConsumedTotal  *prometheus.CounterVec
	EventsValidTotal     prometheus.Counter
	EventsDLQTotal       *prometheus.CounterVec

	// ClickHouse
	CHInsertDuration  prometheus.Histogram
	CHInsertBatchSize prometheus.Histogram
	CHInsertErrors    prometheus.Counter

	// Kafka producer
	KafkaProduceTotal  *prometheus.CounterVec
	KafkaProduceErrors *prometheus.CounterVec

	// Processing duration
	BatchProcessDuration prometheus.Histogram

	// Session cache
	SessionCacheSize prometheus.Gauge
}

// New registers all metrics and returns the Metrics struct.
func New() *Metrics {
	return &Metrics{
		EventsConsumedTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "event_processor_events_consumed_total",
			Help: "Total number of events consumed from Kafka events.raw.",
		}, []string{"project_id"}),

		EventsValidTotal: promauto.NewCounter(prometheus.CounterOpts{
			Name: "event_processor_events_valid_total",
			Help: "Total number of events that passed validation and were enriched.",
		}),

		EventsDLQTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "event_processor_events_dlq_total",
			Help: "Total number of events sent to DLQ.",
		}, []string{"reason"}),

		CHInsertDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "event_processor_clickhouse_insert_duration_seconds",
			Help:    "Duration of ClickHouse batch insert operations.",
			Buckets: prometheus.DefBuckets,
		}),

		CHInsertBatchSize: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "event_processor_clickhouse_insert_batch_size",
			Help:    "Number of events per ClickHouse batch insert.",
			Buckets: []float64{1, 10, 100, 500, 1000, 2500, 5000, 10000},
		}),

		CHInsertErrors: promauto.NewCounter(prometheus.CounterOpts{
			Name: "event_processor_clickhouse_insert_errors_total",
			Help: "Total number of failed ClickHouse batch inserts.",
		}),

		KafkaProduceTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "event_processor_kafka_produce_total",
			Help: "Total number of messages produced to Kafka.",
		}, []string{"topic"}),

		KafkaProduceErrors: promauto.NewCounterVec(prometheus.CounterOpts{
			Name: "event_processor_kafka_produce_errors_total",
			Help: "Total number of Kafka produce errors.",
		}, []string{"topic"}),

		BatchProcessDuration: promauto.NewHistogram(prometheus.HistogramOpts{
			Name:    "event_processor_batch_process_duration_seconds",
			Help:    "End-to-end duration of a full batch (validate + enrich + insert + produce + commit).",
			Buckets: []float64{0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		}),

		SessionCacheSize: promauto.NewGauge(prometheus.GaugeOpts{
			Name: "event_processor_session_cache_size",
			Help: "Current number of active sessions in the in-memory session cache.",
		}),
	}
}
