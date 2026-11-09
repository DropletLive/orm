package orm

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// NewCDCReader builds the change feed's one consumer. For each fetched batch it
// forwards every change of a listened-to table to orm.entity.<table>, in fetch
// order, then acks.
//
// The application owns the loop, as with every StreamConsumer. Forwards dedup
// on Nats-Msg-Id, so whatever is left unacked is simply redelivered.
func NewCDCReader(engine Engine) StreamConsumer {
	return &cdcReader{engine: engine}
}

type cdcReader struct {
	engine Engine

	mu        sync.Mutex
	nats      NatsConsumer
	bySubject map[Subject]*cdcEntityEntry
}

func (r *cdcReader) resolve() (NatsConsumer, map[Subject]*cdcEntityEntry, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.nats != nil {
		return r.nats, r.bySubject, nil
	}
	reg := r.engine.Registry().(*engineRegistryImplementation)
	pool := r.engine.Nats(reg.cdcStream.NatsPool)
	if pool == nil {
		return nil, nil, fmt.Errorf("nats pool '%s' for the cdc reader not configured", reg.cdcStream.NatsPool)
	}
	nc, err := pool.Consumer(string(CDCReaderName))
	if err != nil {
		return nil, nil, err
	}
	r.bySubject = make(map[Subject]*cdcEntityEntry, len(reg.cdcEntities))
	for _, entry := range reg.cdcEntities {
		if entry.cdc {
			r.bySubject[entry.subject] = entry
		}
	}
	r.nats = nc

	return r.nats, r.bySubject, nil
}

func (r *cdcReader) Consume(ctx context.Context, batch int, timeout time.Duration) error {
	if batch <= 0 {
		batch = 1
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	orm := r.engine.NewContext(ctx)
	nc, bySubject, err := r.resolve()
	if err != nil {
		return err
	}
	fetched := nc.Fetch(orm, batch, timeout)
	if fetchErr := fetched.Error(); fetchErr != nil {
		return fetchErr
	}

	reg := r.engine.Registry().(*engineRegistryImplementation)
	metrics, hasMetrics := reg.getMetricsRegistry()

	records := fetched.Records()
	forwards := make([]*NatsMessage, 0, len(records))
	for _, msg := range records {
		entry := bySubject[Subject(msg.Subject)]
		if entry == nil {
			continue
		}
		if hasMetrics {
			metrics.cdcMessages.WithLabelValues(string(CDCReaderName), entry.entityName, cdcOpLabel(msg.Data)).Inc()
			if !msg.Timestamp.IsZero() {
				metrics.streamLag.WithLabelValues(string(CDCReaderName)).Observe(time.Since(msg.Timestamp).Seconds())
			}
		}
		op, _, parseErr := parseCDCIdentity(msg.Data)
		if parseErr != nil {
			// Bytes that never parse would block the feed forever if redelivered.
			r.logError(orm, msg, fmt.Errorf("dropped undecodable cdc event: %w", parseErr))
			continue
		}
		// A snapshot read is not a change; replays reach listeners through Enqueue.
		if op == CDCOpRead {
			continue
		}
		forwards = append(forwards, newEntityMessage(EntitySubject(entry.tableName), msg.Data))
	}

	if len(forwards) > 0 {
		pool := r.engine.Nats(reg.entityStream.NatsPool)
		if pool == nil {
			return fmt.Errorf("nats pool '%s' for the entity stream not configured", reg.entityStream.NatsPool)
		}
		// Nothing is acked: the whole batch comes back, its forwards dedup, and
		// the error makes the caller back off instead of fetching past it.
		if publishErr := pool.PublishBatch(orm, forwards); publishErr != nil {
			return fmt.Errorf("forward %d cdc events: %w", len(forwards), publishErr)
		}
	}
	for _, msg := range records {
		if ackErr := msg.Ack(); ackErr != nil {
			r.logError(orm, msg, fmt.Errorf("ack: %w", ackErr))
		}
	}

	return nil
}

func (r *cdcReader) logError(ctx Context, msg *NatsMessage, err error) {
	_, loggers := ctx.getNatsLoggers()
	if len(loggers) == 0 {
		return
	}
	fillLogFields(ctx, loggers, string(CDCReaderName), sourceNats, "CDC_READ", "subject: "+msg.Subject, nil, false, err)
}
