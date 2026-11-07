package orm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A durable name is unique per stream, not per broker. After a stream rename
// the old stream keeps durables with the same names; a consumer must still read
// the stream it was declared on, not the first stream that has its name.
func TestConsumerFetchesFromItsOwnStreamWhenDurableNameRepeats(t *testing.T) {
	const (
		oldStream = "TEST_DUP_DURABLE_A"
		newStream = "TEST_DUP_DURABLE_B"
		durable   = "test-dup-durable"
	)

	registry := NewRegistry()
	registry.RegisterNatsStream(NewNatsStream(oldStream, "nats").Subjects("test.dupdurable.a.>"))
	registry.RegisterNatsStream(NewNatsStream(newStream, "nats").Subjects("test.dupdurable.b.>"))
	registry.RegisterNatsConsumer(NewNatsConsumer(durable, "nats").
		Stream(newStream).
		FilterSubjects("test.dupdurable.b.>"))
	orm := PrepareTablesWithNats(t, registry)

	js, err := orm.Engine().Nats("nats").GetJetStream()
	require.NoError(t, err)
	for _, name := range []string{oldStream, newStream} {
		stream, sErr := js.Stream(orm.Context(), name)
		require.NoError(t, sErr)
		require.NoError(t, stream.Purge(orm.Context()))
	}
	oldConsumer := NewNatsConsumer(durable, "nats").FilterSubjects("test.dupdurable.a.>").toJetStreamConfig()
	_, err = js.CreateOrUpdateConsumer(orm.Context(), oldStream, oldConsumer)
	require.NoError(t, err)
	t.Cleanup(func() { _ = js.DeleteConsumer(orm.Context(), oldStream, durable) })

	msg := NewNatsMessage("test.dupdurable.b.one")
	msg.Data = []byte(`{"n":1}`)
	require.NoError(t, orm.Engine().Nats("nats").Publish(orm, msg))

	consumer, err := orm.Engine().Nats("nats").Consumer(durable)
	require.NoError(t, err)
	batch := consumer.Fetch(orm, 1, 2*time.Second)
	require.NoError(t, batch.Error())
	records := batch.Records()
	require.Len(t, records, 1)
	assert.Equal(t, "test.dupdurable.b.one", records[0].Subject)
	assert.NoError(t, records[0].Ack())
}
