package orm

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// CDCBatchHandler handles one fetched batch of raw change events, in fetch
// order. Returning nil acks the whole batch; an error acks nothing, so the batch
// comes back and every step of the handler must be idempotent.
type CDCBatchHandler func(ctx Context, events []*NatsMessage) error

// NewCDCReader builds the change feed's one consumer, durable orm-cdc-reader on
// the whole feed. For each fetched batch it calls handle, then acks. Pass
// DispatchCDC to forward the changes to the listeners; an application with more
// to do per change calls DispatchCDC from its own handler.
//
// The application owns the loop, as with every StreamConsumer.
func NewCDCReader(engine Engine, handle CDCBatchHandler) StreamConsumer {
	return &cdcReader{engine: engine, handle: handle}
}

type cdcReader struct {
	engine Engine
	handle CDCBatchHandler

	mu   sync.Mutex
	nats NatsConsumer
}

func (r *cdcReader) resolve() (NatsConsumer, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.nats != nil {
		return r.nats, nil
	}
	reg := r.engine.Registry().(*engineRegistryImplementation)
	pool := r.engine.Nats(reg.cdcStream.NatsPool)
	if pool == nil {
		return nil, fmt.Errorf("nats pool '%s' for the cdc reader not configured", reg.cdcStream.NatsPool)
	}
	nc, err := pool.Consumer(string(CDCReaderName))
	if err != nil {
		return nil, err
	}
	r.nats = nc

	return r.nats, nil
}

func (r *cdcReader) Consume(ctx context.Context, batch int, timeout time.Duration) error {
	if batch <= 0 {
		batch = 1
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	orm := r.engine.NewContext(ctx)
	nc, err := r.resolve()
	if err != nil {
		return err
	}
	fetched := nc.Fetch(orm, batch, timeout)
	if fetchErr := fetched.Error(); fetchErr != nil {
		return fetchErr
	}
	records := fetched.Records()
	if len(records) == 0 {
		return nil
	}

	if metrics, hasMetrics := r.engine.Registry().(*engineRegistryImplementation).getMetricsRegistry(); hasMetrics {
		for _, msg := range records {
			if !msg.Timestamp.IsZero() {
				metrics.streamLag.WithLabelValues(string(CDCReaderName)).Observe(time.Since(msg.Timestamp).Seconds())
			}
		}
	}
	// Nothing is acked on error: the whole batch comes back, and the error makes
	// the caller back off instead of fetching past it.
	if err := r.handle(orm, records); err != nil {
		return err
	}
	for _, msg := range records {
		if ackErr := msg.Ack(); ackErr != nil {
			logCDCError(orm, msg, fmt.Errorf("ack: %w", ackErr))
		}
	}

	return nil
}

// DispatchCDC forwards each change of a table some consumer declares to
// orm.entity.<table>, bytes unchanged and in fetch order across tables. Changes
// of other tables, snapshot reads and undecodable bytes are skipped. A redelivered
// batch dedups on Nats-Msg-Id.
func DispatchCDC(ctx Context, events []*NatsMessage) error {
	reg := ctx.Engine().Registry().(*engineRegistryImplementation)
	metrics, hasMetrics := reg.getMetricsRegistry()

	forwards := make([]*NatsMessage, 0, len(events))
	for _, msg := range events {
		entry := reg.cdcBySubject[Subject(msg.Subject)]
		if entry == nil {
			continue
		}
		if hasMetrics {
			metrics.cdcMessages.WithLabelValues(string(CDCReaderName), entry.entityName, cdcOpLabel(msg.Data)).Inc()
		}
		op, _, parseErr := parseCDCIdentity(msg.Data)
		if parseErr != nil {
			// Bytes that never parse would block the feed forever if redelivered.
			logCDCError(ctx, msg, fmt.Errorf("dropped undecodable cdc event: %w", parseErr))
			continue
		}
		// A snapshot read is not a change; replays reach listeners through Enqueue.
		if op == CDCOpRead {
			continue
		}
		forwards = append(forwards, newEntityMessage(EntitySubject(entry.tableName), msg.Data))
	}
	if len(forwards) == 0 {
		return nil
	}
	pool := ctx.Engine().Nats(reg.entityStream.NatsPool)
	if pool == nil {
		return fmt.Errorf("nats pool '%s' for the entity stream not configured", reg.entityStream.NatsPool)
	}
	if err := pool.PublishBatch(ctx, forwards); err != nil {
		return fmt.Errorf("forward %d cdc events: %w", len(forwards), err)
	}

	return nil
}

func logCDCError(ctx Context, msg *NatsMessage, err error) {
	_, loggers := ctx.getNatsLoggers()
	if len(loggers) == 0 {
		return
	}
	fillLogFields(ctx, loggers, string(CDCReaderName), sourceNats, "CDC_READ", "subject: "+msg.Subject, nil, false, err)
}
