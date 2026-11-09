package orm

import (
	"sort"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// CDCStreamOptions tunes the change feed and its reader. Registering it is
// optional; the defaults match the stream Debezium's deployment creates.
type CDCStreamOptions struct {
	// Producer is who writes the feed: the ORM (default) or Debezium.
	Producer CDCProducer
	Storage  jetstream.StorageType
	Replicas int
	MaxAge   time.Duration
	MaxBytes int64
	// DuplicateWindow dedups the ORM's own retries and the outbox relay; the
	// relay's grace period must stay under it.
	DuplicateWindow time.Duration
	NatsPool        string
	// ReaderAckWait must exceed the slowest forward of one fetched batch.
	ReaderAckWait       time.Duration
	ReaderMaxAckPending int
}

func applyCDCStreamDefaults(opts CDCStreamOptions, pool string) CDCStreamOptions {
	opts.NatsPool = pool
	if opts.Replicas <= 0 {
		opts.Replicas = 1
	}
	if opts.MaxAge <= 0 {
		opts.MaxAge = 7 * 24 * time.Hour
	}
	if opts.DuplicateWindow <= 0 {
		opts.DuplicateWindow = 2 * time.Minute
	}
	if opts.ReaderAckWait <= 0 {
		opts.ReaderAckWait = 30 * time.Second
	}
	if opts.ReaderMaxAckPending == 0 {
		opts.ReaderMaxAckPending = 256
	}

	return opts
}

func (r *registry) RegisterCDCStream(opts CDCStreamOptions) {
	r.cdcStream = &opts
}

// cdcStreamPool is the feed's NATS pool: its own if registered, else the
// entity stream's, so one pool setting covers both.
func cdcStreamPool(r *registry) string {
	if opts := cdcStreamOptionsOf(r); opts.NatsPool != "" {
		return opts.NatsPool
	}
	if opts := entityStreamOptionsOf(r); opts.NatsPool != "" {
		return opts.NatsPool
	}
	return DefaultPoolCode
}

func cdcStreamOptionsOf(r *registry) CDCStreamOptions {
	if r.cdcStream == nil {
		return CDCStreamOptions{}
	}

	return *r.cdcStream
}

// resolveCDCStream applies the defaults and seeds the reader's durable on its
// pool, so pool.Consumer(CDCReaderName) resolves at runtime.
func resolveCDCStream(r *registry, e *engineImplementation) {
	reg := e.registry
	reg.cdcStream = applyCDCStreamDefaults(cdcStreamOptionsOf(r), cdcStreamPool(r))
	subjects := reg.cdcReaderSubjects()
	if len(subjects) == 0 {
		return
	}
	pool, has := r.natsPools[reg.cdcStream.NatsPool]
	if !has {
		return
	}
	pool.consumers[string(CDCReaderName)] = &NatsConsumerSettings{
		Name:           string(CDCReaderName),
		Stream:         CDCStreamName,
		FilterSubjects: subjectStrings(subjects),
		AckWait:        reg.cdcStream.ReaderAckWait,
		MaxAckPending:  reg.cdcStream.ReaderMaxAckPending,
		MaxDeliver:     -1,
	}
}

// cdcReaderSubjects lists every table the reader filters, sorted so the durable
// config is stable across runs.
func (reg *engineRegistryImplementation) cdcReaderSubjects() []Subject {
	subjects := make([]Subject, 0, len(reg.cdcEntities))
	for _, entry := range reg.cdcEntities {
		if entry.cdc {
			subjects = append(subjects, entry.subject)
		}
	}
	sortSubjects(subjects)

	return subjects
}

func (reg *engineRegistryImplementation) cdcStreamConfig() jetstream.StreamConfig {
	opts := reg.cdcStream
	builder := NewNatsStream(CDCStreamName, opts.NatsPool).
		Subjects(cdcSubjectWildcard).
		Duplicates(opts.DuplicateWindow).
		Replicas(opts.Replicas).
		MaxAge(opts.MaxAge).
		MaxBytes(opts.MaxBytes)
	if opts.Storage != 0 {
		builder.Storage(opts.Storage)
	}

	return builder.toConfig()
}

func (reg *engineRegistryImplementation) cdcReaderBuilder() *NatsConsumerBuilder {
	opts := reg.cdcStream

	return NewNatsConsumer(string(CDCReaderName), opts.NatsPool).
		Stream(CDCStreamName).
		FilterSubjects(subjectStrings(reg.cdcReaderSubjects())...).
		MaxAckPending(opts.ReaderMaxAckPending).
		AckWait(opts.ReaderAckWait).
		MaxDeliver(-1)
}

func sortSubjects(subjects []Subject) {
	sort.Slice(subjects, func(i, j int) bool { return subjects[i] < subjects[j] })
}
