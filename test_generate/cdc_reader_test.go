package test_generate

import (
	"strconv"
	"testing"
	"time"

	"github.com/DropletLive/orm"
	"github.com/DropletLive/orm/test_generate/entities"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func cdcStreamMessages(t *testing.T, ctx orm.Context) uint64 {
	t.Helper()
	js, err := ctx.Engine().Nats("nats").GetJetStream()
	require.NoError(t, err)
	stream, err := js.Stream(ctx.Context(), orm.CDCStreamName)
	require.NoError(t, err)
	info, err := stream.Info(ctx.Context())
	require.NoError(t, err)

	return info.State.Msgs
}

// A snapshot read is not a change: Debezium's snapshot events stay on the feed
// and reach no listener.
func TestReadEventsReachNoListener(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	calls := 0
	consumer := withReader(ctx.Engine(), orm.NewConsumer(ctx.Engine(), entities.ConsumerTestIndexer).
		OnGenerateEntityDirtyB(func(orm.Context, *entities.GenerateEntityDirtyBDirtyEvent) error {
			calls++

			return nil
		}).
		Build())

	e := entities.GenerateEntityDirtyBProvider.New(ctx)
	e.SetLabel("written")
	require.NoError(t, ctx.Save(e))
	drainUntil(t, consumer, func() bool { return calls == 1 }, 30*time.Second)
	require.Equal(t, 1, calls)

	snapshot := orm.NewNatsMessage(string(orm.CDCSubject("test", "generateEntityDirtyB")))
	snapshot.Data = []byte(`{"before":null,"after":{"ID":` + strconv.FormatUint(e.GetID(), 10) +
		`,"Label":"written"},"op":"r","ts_ms":1,"source":{"ts_ms":1}}`)
	require.NoError(t, ctx.Engine().Nats("nats").Publish(ctx, snapshot))
	for range 3 {
		require.NoError(t, consumer.Consume(ctx.Context(), 16, 300*time.Millisecond))
	}

	assert.Equal(t, 1, calls, "a read event is not forwarded to listeners")
	pending, err := orm.ConsumerPending(ctx, orm.CDCReaderName)
	require.NoError(t, err)
	assert.Zero(t, pending, "the read event is acked")
}

// Once Debezium owns the feed the ORM must publish nothing.
func TestExternalProducerPublishesNothing(t *testing.T) {
	registry := FixtureRegistry()
	registry.RegisterCDCStream(orm.CDCStreamOptions{Producer: orm.CDCProducerExternal, NatsPool: "nats"})
	ctx := orm.PrepareTablesWithConsumers(t, registry, FixtureConsumers(),
		generateEntityOutbox{}, generateEntityStoreOnly{}, generateEntityDirty{},
		generateEntityDirtyB{}, orm.CDCOutboxEntity{}, orm.JobRunEntity{},
	)
	defer ctx.Engine().Nats("nats").Close()

	before := cdcStreamMessages(t, ctx)
	e := entities.GenerateEntityDirtyBProvider.New(ctx)
	e.SetLabel("external")
	require.NoError(t, ctx.Save(e))
	assert.Equal(t, before, cdcStreamMessages(t, ctx), "Debezium writes the feed, not the ORM")
}

// A snapshot built for a test reads back like one decoded from the feed.
func TestNewSnapshotIsReadOnlyAndReadable(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	e := entities.GenerateEntityDirtyBProvider.NewSnapshot(42, func(e *entities.GenerateEntityDirtyB) {
		e.SetLabel("snap")
	})
	assert.Equal(t, uint64(42), e.GetID())
	assert.Equal(t, "snap", e.GetLabel())
	assert.Equal(t, "snap", e.PrivateColumnValue("Label"))
	assert.Error(t, ctx.Save(e), "a snapshot is never saved")
}

// The reader's backlog is readable like any consumer's, so the application can
// alert on a feed nobody is draining.
func TestReaderBacklogIsReadable(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	e := entities.GenerateEntityDirtyBProvider.New(ctx)
	e.SetLabel("backlog")
	require.NoError(t, ctx.Save(e))

	pending, err := orm.ConsumerPending(ctx, orm.CDCReaderName)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), pending)

	require.NoError(t, pumpReader(ctx))
	pending, err = orm.ConsumerPending(ctx, orm.CDCReaderName)
	require.NoError(t, err)
	assert.Zero(t, pending)
}

// The reader forwards in fetch order, so a listener reading several tables sees
// them in the order they were written.
func TestReaderForwardsInWriteOrderAcrossTables(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	a := entities.GenerateEntityDirtyProvider.New(ctx)
	a.SetName("first")
	a.SetAge(1)
	require.NoError(t, ctx.Save(a))
	b := entities.GenerateEntityDirtyBProvider.New(ctx)
	b.SetLabel("second")
	require.NoError(t, ctx.Save(b))
	a.SetAge(2)
	require.NoError(t, ctx.Save(a))
	require.NoError(t, pumpReader(ctx))

	js, err := ctx.Engine().Nats("nats").GetJetStream()
	require.NoError(t, err)
	stream, err := js.Stream(ctx.Context(), orm.EntityStreamName)
	require.NoError(t, err)
	info, err := stream.Info(ctx.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(3), info.State.Msgs)

	var subjects []string
	for seq := info.State.FirstSeq; seq <= info.State.LastSeq; seq++ {
		msg, getErr := stream.GetMsg(ctx.Context(), seq)
		require.NoError(t, getErr)
		subjects = append(subjects, msg.Subject)
	}
	assert.Equal(t, []string{
		string(orm.EntitySubject("generateEntityDirty")),
		string(orm.EntitySubject("generateEntityDirtyB")),
		string(orm.EntitySubject("generateEntityDirty")),
	}, subjects)
}

// A forward that fails acks nothing and returns the error, so the loop backs
// off instead of fetching newer changes past the ones it could not deliver.
func TestReaderReturnsAFailedForward(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	e := entities.GenerateEntityDirtyProvider.New(ctx)
	e.SetName("undelivered")
	e.SetAge(1)
	require.NoError(t, ctx.Save(e))

	js, err := ctx.Engine().Nats("nats").GetJetStream()
	require.NoError(t, err)
	require.NoError(t, js.DeleteStream(ctx.Context(), orm.EntityStreamName))

	assert.Error(t, pumpReader(ctx))
	consumer, err := js.Consumer(ctx.Context(), orm.CDCStreamName, string(orm.CDCReaderName))
	require.NoError(t, err)
	info, err := consumer.Info(ctx.Context())
	require.NoError(t, err)
	assert.Equal(t, 1, info.NumAckPending, "the change stays on the feed for redelivery")
}
