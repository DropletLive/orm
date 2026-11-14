package orm

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func prepareReconnectTest(t *testing.T, stream, durable, subjects string) Context {
	t.Helper()
	registry := NewRegistry()
	registry.RegisterNatsStream(NewNatsStream(stream, "nats").Subjects(subjects))
	registry.RegisterNatsConsumer(NewNatsConsumer(durable, "nats").Stream(stream).FilterSubjects(subjects))
	orm := PrepareTablesWithNats(t, registry)

	js, err := orm.Engine().Nats("nats").GetJetStream()
	require.NoError(t, err)
	s, err := js.Stream(orm.Context(), stream)
	require.NoError(t, err)
	require.NoError(t, s.Purge(orm.Context()))
	return orm
}

func publishAndFetchOne(t *testing.T, orm Context, consumer NatsConsumer, subject string) {
	t.Helper()
	msg := NewNatsMessage(subject)
	msg.Data = []byte(`{}`)
	require.NoError(t, orm.Engine().Nats("nats").Publish(orm, msg))

	batch := consumer.Fetch(orm, 1, 2*time.Second)
	require.NoError(t, batch.Error())
	records := batch.Records()
	require.Len(t, records, 1)
	assert.Equal(t, subject, records[0].Subject)
	assert.NoError(t, records[0].Ack())
}

// A consumer handle outlives the connection it was made on: once the pool replaces a closed
// connection, Fetch must follow it instead of failing with nats.ErrConnectionClosed for good.
func TestConsumerFetchFollowsReplacedConnection(t *testing.T) {
	const subjects = "test.reconnect.closed.>"
	orm := prepareReconnectTest(t, "TEST_RECONNECT_CLOSED", "test-reconnect-closed", subjects)
	pool := orm.Engine().Nats("nats")

	consumer, err := pool.Consumer("test-reconnect-closed")
	require.NoError(t, err)
	publishAndFetchOne(t, orm, consumer, "test.reconnect.closed.one")

	old, err := pool.GetConn()
	require.NoError(t, err)
	old.Close()

	publishAndFetchOne(t, orm, consumer, "test.reconnect.closed.two")
	current, err := pool.GetConn()
	require.NoError(t, err)
	assert.False(t, old == current, "the closed connection was not replaced")
}

// A connection that is reconnecting belongs to nats.go: the pool must hand it out unchanged
// rather than close it and open another under every handle made from it.
func TestPoolKeepsReconnectingConnection(t *testing.T) {
	const subjects = "test.reconnect.blip.>"
	orm := prepareReconnectTest(t, "TEST_RECONNECT_BLIP", "test-reconnect-blip", subjects)
	pool := orm.Engine().Nats("nats")

	consumer, err := pool.Consumer("test-reconnect-blip")
	require.NoError(t, err)
	publishAndFetchOne(t, orm, consumer, "test.reconnect.blip.one")

	nc, err := pool.GetConn()
	require.NoError(t, err)
	require.NoError(t, nc.ForceReconnect())

	deadline := time.Now().Add(5 * time.Second)
	for nc.Status() != nats.CONNECTED {
		require.True(t, time.Now().Before(deadline), "connection did not come back")
		current, cErr := pool.GetConn()
		require.NoError(t, cErr)
		require.True(t, nc == current, "the pool replaced a reconnecting connection")
		time.Sleep(time.Millisecond)
	}
	require.False(t, nc.IsClosed())

	publishAndFetchOne(t, orm, consumer, "test.reconnect.blip.two")
	current, err := pool.GetConn()
	require.NoError(t, err)
	assert.True(t, nc == current, "the pool replaced the reconnected connection")
}
