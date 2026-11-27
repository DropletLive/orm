package test_generate

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/DropletLive/orm"
	"github.com/DropletLive/orm/test_generate/entities"
	"github.com/stretchr/testify/assert"
)

// outboxOnSecondPool exists only to prove Validate rejects an outbox that cannot
// share the source write's transaction. It is not in FixtureEntities, so it has
// no generated code and never reaches a query.
type outboxOnSecondPool struct {
	ID   uint64 `orm:"mysql=second;outbox"`
	Name string `orm:"required;length=100"`
}

func outboxCtx(t *testing.T) orm.Context {
	t.Helper()

	return orm.PrepareTablesWithConsumers(t,
		FixtureRegistry(),
		FixtureConsumers(),
		generateEntityOutbox{}, generateEntityStoreOnly{}, generateEntityDirty{},
		generateEntityDirtyB{}, orm.OutboxEntity{}, orm.JobRunEntity{},
	)
}

func outboxRows(t *testing.T, ctx orm.Context) []*entities.Outbox {
	t.Helper()

	rows, err := entities.OutboxProvider.SearchMany(ctx, orm.NewQuery().
		SortByASC(entities.OutboxProvider.Fields.ID).
		Pager(orm.NewPager(1, 100)))
	assert.NoError(t, err)

	return rows
}

// onlyOutboxRow fails the test outright when the row count is wrong, so a
// staging regression reports as a clean assertion rather than an index panic.
func onlyOutboxRow(t *testing.T, ctx orm.Context) *entities.Outbox {
	t.Helper()

	rows := outboxRows(t, ctx)
	if !assert.Len(t, rows, 1) {
		t.FailNow()
	}

	return rows[0]
}

func newOutboxEntity(ctx orm.Context, name string) *entities.GenerateEntityOutbox {
	e := entities.GenerateEntityOutboxProvider.New(ctx)
	e.SetName(name)
	e.SetAge(30)

	return e
}

func TestOutboxRowRollsBackWithTheEntity(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	err := ctx.Transaction(func(tx orm.Context) error {
		if saveErr := tx.Save(newOutboxEntity(tx, "rollback-me")); saveErr != nil {
			return saveErr
		}

		return assert.AnError
	})
	assert.ErrorIs(t, err, assert.AnError)

	assert.Empty(t, outboxRows(t, ctx.Clone()), "outbox row must not survive a rolled back transaction")
	sources, err := entities.GenerateEntityOutboxProvider.SearchMany(ctx.Clone(),
		orm.NewQuery().Pager(orm.NewPager(1, 100)))
	assert.NoError(t, err)
	assert.Empty(t, sources)
}

func TestOutboxRowIsAtomicWithoutAnExplicitTransaction(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	logs := captureQueries(ctx)
	e := newOutboxEntity(ctx, "no-explicit-tx")
	assert.NoError(t, ctx.Save(e))

	// A bare single-entity Save queues two statements, so DatabasePipeline opens
	// its own transaction rather than running them in autocommit.
	assert.Positive(t, logs.count("START TRANSACTION"),
		"entity INSERT and outbox INSERT must share a transaction even without ctx.Transaction")

	assert.Equal(t, e.GetID(), onlyOutboxRow(t, ctx.Clone()).GetEntityID())
}

func TestOutboxRowIsVisibleInsideTheTransaction(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	assert.NoError(t, ctx.Transaction(func(tx orm.Context) error {
		if err := tx.Save(newOutboxEntity(tx, "in-tx")); err != nil {
			return err
		}
		assert.Equal(t, "GenerateEntityOutbox", onlyOutboxRow(t, tx).GetEntityName())

		return nil
	}))
}

// TestOutboxRowRecordsTheChange pins what an audit lookup reads: the entity, the
// row, the operation and the before/after images, for each kind of write.
func TestOutboxRowRecordsTheChange(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	e := newOutboxEntity(ctx, "first")
	assert.NoError(t, ctx.Save(e))
	e.SetName("second")
	assert.NoError(t, ctx.Save(e))
	assert.NoError(t, ctx.Delete(e))

	rows := outboxRows(t, ctx.Clone())
	if !assert.Len(t, rows, 3) {
		t.FailNow()
	}

	type envelope struct {
		Before map[string]any `json:"before"`
		After  map[string]any `json:"after"`
		Op     string         `json:"op"`
	}
	read := func(row *entities.Outbox) envelope {
		var env envelope
		assert.NoError(t, json.Unmarshal([]byte(row.GetPayload()), &env))

		return env
	}

	for i, want := range []orm.DirtyOp{orm.DirtyInsert, orm.DirtyUpdate, orm.DirtyDelete} {
		assert.Equal(t, uint64(want), rows[i].GetOp())
		assert.Equal(t, "GenerateEntityOutbox", rows[i].GetEntityName())
		assert.Equal(t, e.GetID(), rows[i].GetEntityID())
	}

	insert := read(rows[0])
	assert.Equal(t, orm.CDCOpCreate, insert.Op)
	assert.Nil(t, insert.Before)
	assert.Equal(t, "first", insert.After["Name"])

	update := read(rows[1])
	assert.Equal(t, orm.CDCOpUpdate, update.Op)
	assert.Equal(t, "first", update.Before["Name"])
	assert.Equal(t, "second", update.After["Name"])

	deleted := read(rows[2])
	assert.Equal(t, orm.CDCOpDelete, deleted.Op)
	assert.Equal(t, "second", deleted.Before["Name"])
	assert.Nil(t, deleted.After)
}

func TestStoreOnlyEntityWritesARow(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	e := entities.GenerateEntityStoreOnlyProvider.New(ctx)
	e.SetName("store-only")
	assert.NoError(t, ctx.Save(e))

	assert.Contains(t, onlyOutboxRow(t, ctx.Clone()).GetPayload(), "store-only",
		"an outbox entity no consumer declares still gets its row and its generated row images")
}

func TestConsumedEntityWithoutOutboxWritesNoRow(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	e := entities.GenerateEntityDirtyProvider.New(ctx)
	e.SetName("dirty-only")
	e.SetAge(7)
	assert.NoError(t, ctx.Save(e))

	assert.Empty(t, outboxRows(t, ctx.Clone()))
}

func TestOneWriteIsOneRow(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	assert.NoError(t, ctx.Save(
		newOutboxEntity(ctx, "row-a"),
		newOutboxEntity(ctx, "row-b"),
		newOutboxEntity(ctx, "row-c"),
	))

	assert.Len(t, outboxRows(t, ctx.Clone()), 3)
}

func TestOutboxWritesNothingToNats(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	published := &natsOperationLog{}
	ctx.RegisterQueryLogger(published, orm.QueryLoggerOptions{Nats: true})

	assert.NoError(t, ctx.Save(newOutboxEntity(ctx, "silent")))
	assert.Len(t, outboxRows(t, ctx.Clone()), 1)
	assert.Zero(t, published.publishes, "Debezium owns the change feed; a save publishes nothing")
}

func TestPurgeRemovesRowsPastRetention(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	assert.NoError(t, ctx.Save(newOutboxEntity(ctx, "old")))
	storeOnly := entities.GenerateEntityStoreOnlyProvider.New(ctx)
	storeOnly.SetName("also-old")
	assert.NoError(t, ctx.Save(storeOnly))

	deleted, err := orm.PurgeOutbox(ctx, time.Hour, 100)
	assert.NoError(t, err)
	assert.Zero(t, deleted, "rows inside the retention window stay")

	// A negative window puts the cutoff in the future, so every row qualifies
	// without the test having to fake a clock.
	deleted, err = orm.PurgeOutbox(ctx, -time.Minute, 100)
	assert.NoError(t, err)
	assert.Equal(t, 2, deleted)
	assert.Empty(t, outboxRows(t, ctx.Clone()))
}

func TestPurgeStopsAtTheLimit(t *testing.T) {
	ctx := outboxCtx(t)
	defer ctx.Engine().Nats("nats").Close()

	assert.NoError(t, ctx.Save(newOutboxEntity(ctx, "a"), newOutboxEntity(ctx, "b"), newOutboxEntity(ctx, "c")))

	deleted, err := orm.PurgeOutbox(ctx, -time.Minute, 2)
	assert.NoError(t, err)
	assert.Equal(t, 2, deleted)
	assert.Len(t, outboxRows(t, ctx.Clone()), 1)
}

func TestValidateRejectsOutboxWithoutTheEntityRegistered(t *testing.T) {
	registry := orm.NewRegistry()
	registry.RegisterMySQL("root:root@tcp(localhost:3397)/test", "default", &orm.MySQLOptions{})
	registry.RegisterRedis("localhost:6395", 0, "default", nil)
	registry.RegisterEntity(generateEntityStoreOnly{})

	_, err := registry.Validate()
	assert.ErrorContains(t, err, "orm.OutboxEntity is not registered")
	assert.ErrorContains(t, err, "generateEntityStoreOnly")
}

func TestValidateRejectsAnOutboxInAnotherPool(t *testing.T) {
	registry := orm.NewRegistry()
	registry.RegisterMySQL("root:root@tcp(localhost:3397)/test", "default", &orm.MySQLOptions{})
	registry.RegisterMySQL("root:root@tcp(localhost:3397)/test", "second", &orm.MySQLOptions{})
	registry.RegisterRedis("localhost:6395", 0, "default", nil)
	registry.RegisterEntity(outboxOnSecondPool{}, orm.OutboxEntity{})

	_, err := registry.Validate()
	assert.ErrorContains(t, err, "would not be in the same transaction")
}

// queryLog records every MySQL statement executed on a context.
type queryLog struct{ queries []string }

func (q *queryLog) Handle(_ orm.Context, log map[string]any) {
	if s, ok := log["query"].(string); ok {
		q.queries = append(q.queries, s)
	}
}

func (q *queryLog) count(fragment string) int {
	n := 0
	for _, s := range q.queries {
		if strings.Contains(s, fragment) {
			n++
		}
	}

	return n
}

func captureQueries(ctx orm.Context) *queryLog {
	log := &queryLog{}
	ctx.RegisterQueryLogger(log, orm.QueryLoggerOptions{MySQL: true})

	return log
}

// natsOperationLog counts NATS publishes on a context.
type natsOperationLog struct{ publishes int }

func (l *natsOperationLog) Handle(_ orm.Context, log map[string]any) {
	if op, _ := log["operation"].(string); strings.HasPrefix(op, "PUBLISH") {
		l.publishes++
	}
}
