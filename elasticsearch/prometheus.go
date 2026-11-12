package elasticsearch

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const (
	esLabelOperation = "operation"
	esLabelIndex     = "index"
	esLabelOutcome   = "outcome"

	opSearch         = "search"
	opIndexDocument  = "index_document"
	opDeleteDocument = "delete_document"
	opCount          = "count"
	opIndexExists    = "index_exists"
	opCreateIndex    = "create_index"
	opDeleteIndex    = "delete_index"

	outcomeSuccess  = "success"
	outcomeError    = "error"
	outcomeNotFound = "not_found"
)

var (
	esRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "droplet_elasticsearch_requests_total",
		Help: "Elasticsearch requests by operation, index and outcome",
	}, []string{esLabelOperation, esLabelIndex, esLabelOutcome})

	esRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "droplet_elasticsearch_request_duration_seconds",
		Help:    "Duration of Elasticsearch requests",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
	}, []string{esLabelOperation, esLabelIndex})

	esSearchHits = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "droplet_elasticsearch_search_hits",
		Help:    "Total hits reported by Elasticsearch per search",
		Buckets: prometheus.ExponentialBuckets(1, 4, 8),
	}, []string{esLabelIndex})

	esBulkSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "droplet_elasticsearch_bulk_size",
		Help:    "Number of documents per Elasticsearch bulk request",
		Buckets: prometheus.ExponentialBuckets(1, 2, 12),
	}, []string{esLabelOperation, esLabelIndex})
)

func observe(operation, index string, start time.Time, outcome string) {
	esRequestsTotal.With(prometheus.Labels{
		esLabelOperation: operation,
		esLabelIndex:     index,
		esLabelOutcome:   outcome,
	}).Inc()

	esRequestDuration.With(prometheus.Labels{
		esLabelOperation: operation,
		esLabelIndex:     index,
	}).Observe(time.Since(start).Seconds())
}

func observeBulkSize(operation, index string, size int) {
	esBulkSize.With(prometheus.Labels{
		esLabelOperation: operation,
		esLabelIndex:     index,
	}).Observe(float64(size))
}

func outcomeOf(err error) string {
	if err != nil {
		return outcomeError
	}

	return outcomeSuccess
}
