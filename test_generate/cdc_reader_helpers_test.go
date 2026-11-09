package test_generate

import (
	"context"
	"testing"
	"time"

	"github.com/DropletLive/orm"
)

// throughReader drives the CDC reader before the consumer, the way production
// runs both: a write lands on the change feed, and only the reader forwards it
// to the entity stream the consumer reads.
type throughReader struct {
	reader   orm.StreamConsumer
	consumer orm.StreamConsumer
}

func withReader(engine orm.Engine, consumer orm.StreamConsumer) orm.StreamConsumer {
	return throughReader{reader: orm.NewCDCReader(engine), consumer: consumer}
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
	return orm.NewCDCReader(ctx.Engine()).Consume(ctx.Context(), 64, 300*time.Millisecond)
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
