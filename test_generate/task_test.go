package test_generate

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/DropletLive/orm"
	"github.com/DropletLive/orm/test_fixtures/jobtasks"
	mediatasks "github.com/DropletLive/orm/test_fixtures/media/jobtasks"
	"github.com/DropletLive/orm/test_generate/entities"
)

func taskFixtureContext(t *testing.T) orm.Context {
	t.Helper()

	return orm.PrepareTablesWithConsumers(
		t, FixtureRegistry(), FixtureTaskConsumers(), orm.JobRunEntity{})
}

// The end-to-end proof that the three generated pieces agree: the typed
// dispatch function, the typed builder method, and the consumer they share. If
// the generator emitted a different task name on either side, the message would
// be dead-lettered as unhandled instead of arriving here.
func TestGeneratedDispatchAndHandlerRoundTrip(t *testing.T) {
	ctx := taskFixtureContext(t)
	defer ctx.Engine().Nats("nats").Close()

	var (
		mu       sync.Mutex
		welcomes []*jobtasks.SendWelcomeEmail
		receipts []*jobtasks.SendReceipt
	)

	consumer := orm.NewConsumer(ctx.Engine(), entities.ConsumerEmailsWorker).
		OnSendWelcomeEmail(func(_ orm.Context, task *jobtasks.SendWelcomeEmail) error {
			mu.Lock()
			defer mu.Unlock()
			welcomes = append(welcomes, task)

			return nil
		}).
		OnSendReceipt(func(_ orm.Context, task *jobtasks.SendReceipt) error {
			mu.Lock()
			defer mu.Unlock()
			receipts = append(receipts, task)

			return nil
		}).
		Build()

	welcomeID, err := entities.DispatchSendWelcomeEmail(ctx, &jobtasks.SendWelcomeEmail{UserID: 42, Locale: "pl"})
	require.NoError(t, err)

	receiptID, err := entities.DispatchSendReceipt(ctx, &jobtasks.SendReceipt{OrderID: 99})
	require.NoError(t, err)

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		require.NoError(t, consumer.Consume(context.Background(), 10, 200*time.Millisecond))

		mu.Lock()
		done := len(welcomes) == 1 && len(receipts) == 1
		mu.Unlock()

		if done {
			break
		}
	}

	mu.Lock()
	defer mu.Unlock()

	require.Len(t, welcomes, 1)
	assert.Equal(t, uint64(42), welcomes[0].UserID)
	assert.Equal(t, "pl", welcomes[0].Locale)

	require.Len(t, receipts, 1)
	assert.Equal(t, uint64(99), receipts[0].OrderID)

	run, err := entities.JobRunsProvider.GetByID(ctx, welcomeID)
	require.NoError(t, err)
	assert.Equal(t, "SendWelcomeEmail", run.GetTask())
	assert.Equal(t, "emails", run.GetQueue())
	assert.Equal(t, "succeeded", string(run.GetStatus()))
	assert.Equal(t, uint64(1), run.GetAttempts())

	receiptRun, err := entities.JobRunsProvider.GetByID(ctx, receiptID)
	require.NoError(t, err)
	assert.Equal(t, "succeeded", string(receiptRun.GetStatus()))
}

// TestTaskQueueComesFromTheInterface pins the optional-interface probe. A probe
// that only checked one receiver form would put half the tasks on the default
// queue, where nothing declared here would drain them.
func TestTaskQueueComesFromTheInterface(t *testing.T) {
	ctx := taskFixtureContext(t)
	defer ctx.Engine().Nats("nats").Close()

	// Value receiver.
	welcomeID, err := entities.DispatchSendWelcomeEmail(ctx, &jobtasks.SendWelcomeEmail{UserID: 1})
	require.NoError(t, err)
	assertRunQueue(t, ctx, welcomeID, "emails")

	// Pointer receiver.
	receiptID, err := entities.DispatchSendReceipt(ctx, &jobtasks.SendReceipt{OrderID: 2})
	require.NoError(t, err)
	assertRunQueue(t, ctx, receiptID, "emails")

	// Another package's task, so aliasing and the queue stay independent.
	clipID, err := entities.DispatchTranscodeClip(ctx, &mediatasks.TranscodeClip{Path: "a.mp4"})
	require.NoError(t, err)
	assertRunQueue(t, ctx, clipID, "media")
}

// TestTaskWithoutQueueFallsBackToDefault covers the task that declares nothing.
func TestTaskWithoutQueueFallsBackToDefault(t *testing.T) {
	ctx := taskFixtureContext(t)
	defer ctx.Engine().Nats("nats").Close()

	runID, err := entities.DispatchSendPasswordReset(ctx, &jobtasks.SendPasswordReset{UserID: 7})
	require.NoError(t, err)
	assertRunQueue(t, ctx, runID, string(orm.DefaultQueue))
}

// TestTaskSubjectIsItsQueueAndName is the routing contract: the subject a
// dispatch publishes to is the one its consumer filters, and nothing else
// carries the task's identity.
func TestTaskSubjectIsItsQueueAndName(t *testing.T) {
	ctx := taskFixtureContext(t)
	defer ctx.Engine().Nats("nats").Close()

	_, err := entities.DispatchTranscodeClip(ctx, &mediatasks.TranscodeClip{Path: "b.mp4"})
	require.NoError(t, err)

	cons, err := ctx.Engine().Nats("nats").Consumer("media-worker")
	require.NoError(t, err)

	defer cons.Close()

	var subjects []string
	deadline := time.Now().Add(10 * time.Second)
	for len(subjects) == 0 && time.Now().Before(deadline) {
		for _, msg := range cons.Fetch(ctx, 10, time.Second).Records() {
			subjects = append(subjects, msg.Subject)
			_ = msg.Ack()
		}
	}

	assert.Equal(t, []string{"fluxa.task.media.TranscodeClip"}, subjects)
}

// TestGeneratedBuilderRefusesAPartialConsumer: a consumer whose tasks are only
// partly handled must refuse to build rather than dead-letter the unhandled
// ones at runtime.
func TestGeneratedBuilderRefusesAPartialConsumer(t *testing.T) {
	ctx := taskFixtureContext(t)
	defer ctx.Engine().Nats("nats").Close()

	assert.PanicsWithValue(t,
		`consumer 'emails-worker': no handler registered for task 'SendReceipt'`,
		func() {
			orm.NewConsumer(ctx.Engine(), entities.ConsumerEmailsWorker).
				OnSendWelcomeEmail(func(_ orm.Context, _ *jobtasks.SendWelcomeEmail) error { return nil }).
				Build()
		})
}

// TestTaskAndEntityConsumersLandOnTheirOwnStream: the two kinds are one concept
// over two streams, and each durable has to be created on the right one.
func TestTaskAndEntityConsumersLandOnTheirOwnStream(t *testing.T) {
	ctx := orm.PrepareTablesWithConsumers(t, FixtureRegistry(), FixtureConsumers(),
		generateEntityDirty{}, generateEntityDirtyB{}, generateEntityOutbox{},
		orm.CDCOutboxEntity{}, orm.JobRunEntity{})
	defer ctx.Engine().Nats("nats").Close()

	js, err := ctx.Engine().Nats("nats").GetJetStream()
	require.NoError(t, err)

	for _, name := range []string{"test-indexer", "test-notifier"} {
		_, consErr := js.Consumer(ctx.Context(), orm.EntityStreamName, name)
		require.NoErrorf(t, consErr, "entity consumer %q must live on the entity stream", name)
	}

	for _, name := range []string{"emails-worker", "media-worker", "default-worker"} {
		_, consErr := js.Consumer(ctx.Context(), orm.TaskStreamName, name)
		require.NoErrorf(t, consErr, "task consumer %q must live on the task stream", name)
	}
}

func assertRunQueue(t *testing.T, ctx orm.Context, runID uint64, queue string) {
	t.Helper()

	run, err := entities.JobRunsProvider.GetByID(ctx.Clone(), runID)
	require.NoError(t, err)
	assert.Equal(t, queue, run.GetQueue())
}
