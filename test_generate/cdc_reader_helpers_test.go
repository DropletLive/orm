package test_generate

import (
	"context"
	"testing"
	"time"

	"github.com/DropletLive/orm"
	"github.com/DropletLive/orm/test_generate/entities"
)

// throughReader drives the CDC reader before the consumer, the way production
// runs both: a write lands on the change feed, and only the reader forwards it
// to the entity stream the consumer reads.
type throughReader struct {
	reader   orm.StreamConsumer
	consumer orm.StreamConsumer
}

func withReader(engine orm.Engine, consumer orm.StreamConsumer) orm.StreamConsumer {
	return throughReader{reader: orm.NewCDCReader(engine, orm.DispatchCDC), consumer: consumer}
}

func (p throughReader) Consume(ctx context.Context, batch int, timeout time.Duration) error {
	if err := p.reader.Consume(ctx, batch, min(timeout, 300*time.Millisecond)); err != nil {
		return err
	}

	return p.consumer.Consume(ctx, batch, timeout)
}

// pumpReader runs the reader alone, for tests that watch the entity stream
// rather than a consumer.
func pumpReader(ctx orm.Context) error {
	return orm.NewCDCReader(ctx.Engine(), orm.DispatchCDC).Consume(ctx.Context(), 64, 300*time.Millisecond)
}

// drainUntil pumps the consumer until done() reports true or the deadline passes.
func drainUntil(t *testing.T, consumer orm.StreamConsumer, done func() bool, deadline time.Duration) {
	t.Helper()
	end := time.Now().Add(deadline)
	for time.Now().Before(end) {
		if done() {
			return
		}
		pumpCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = consumer.Consume(pumpCtx, 32, 1*time.Second)
		cancel()
	}
}

// cdcChangeSource is what every generated entity on the feed implements.
type cdcChangeSource interface {
	PrivateCDCChange(op orm.DirtyOp) orm.CDCChange
	PrivateCacheIndex() string
}

// emulateDebezium stands in for Debezium in tests: after each committed write
// of the given entities it publishes the change to cdc.<database>.<table>, the
// envelope Debezium would read from the binlog. The ORM itself never publishes
// the feed. tables maps a sample entity of each kind to its table name.
func emulateDebezium(t *testing.T, ctx orm.Context, tables map[orm.Entity]string) {
	t.Helper()
	engine := ctx.Engine()
	database := engine.DB(orm.DefaultPoolCode).GetConfig().GetDatabaseName()
	for sample, table := range tables {
		subject := orm.CDCSubject(database, table)
		publish := func(c orm.Context, e orm.Entity, op orm.DirtyOp) error {
			payload, err := orm.MarshalCDCChange(e.(cdcChangeSource).PrivateCDCChange(op), database, table, time.Now())
			if err != nil {
				return err
			}
			msg := orm.NewNatsMessage(string(subject))
			msg.Data = payload

			return c.Engine().Nats("nats").PublishBatch(c, []*orm.NatsMessage{msg})
		}
		cacheIndex := sample.(cdcChangeSource).PrivateCacheIndex()
		orm.RegisterAfterInsertHandler(engine, cacheIndex, func(c orm.Context, e orm.Entity) error {
			return publish(c, e, orm.DirtyInsert)
		})
		orm.RegisterAfterUpdateHandler(engine, cacheIndex, func(c orm.Context, e orm.Entity, _ map[string]any) error {
			return publish(c, e, orm.DirtyUpdate)
		})
		orm.RegisterAfterDeleteHandler(engine, cacheIndex, func(c orm.Context, e orm.Entity) error {
			return publish(c, e, orm.DirtyDelete)
		})
	}
}

// feedCtx is outboxCtx with Debezium stood in for, so a save reaches the feed.
func feedCtx(t *testing.T) orm.Context {
	t.Helper()
	ctx := outboxCtx(t)
	emulateDebezium(t, ctx, map[orm.Entity]string{
		entities.GenerateEntityDirtyProvider.New(ctx):  "generateEntityDirty",
		entities.GenerateEntityDirtyBProvider.New(ctx): "generateEntityDirtyB",
		entities.GenerateEntityOutboxProvider.New(ctx): "generateEntityOutbox",
	})

	return ctx
}
