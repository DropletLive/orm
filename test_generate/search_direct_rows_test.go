package test_generate

import (
	"strings"
	"testing"
	"time"

	"github.com/DropletLive/orm"
	"github.com/DropletLive/orm/test_generate/entities"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newNoRedisRows(t *testing.T, ctx orm.Context, names ...string) []*entities.GenerateEntityNoRedis {
	t.Helper()
	rows := make([]*entities.GenerateEntityNoRedis, 0, len(names))
	now := time.Now().UTC()
	for i, name := range names {
		e := entities.GenerateEntityNoRedisProvider.New(ctx)
		e.SetName(name)
		e.SetAge(uint64(10 + i))
		e.SetTime(now)
		e.SetDate(now)
		require.NoError(t, ctx.Save(e))
		rows = append(rows, e)
	}
	return rows
}

// An entity without a Redis tier has no cache to route through, so SearchMany must read the rows in
// the search query itself: one SELECT, with the ORDER BY and LIMIT applied by MySQL.
func TestSearchManyWithoutRedisReadsRowsInOneQuery(t *testing.T) {
	ctx := orm.PrepareTables(t, orm.NewRegistry(), generateEntityNoRedis{}, generateReferenceEntity{})
	newNoRedisRows(t, ctx, "c", "a", "b")

	reader := ctx.Clone()
	reader.DisableContextCache()
	sql := &orm.MockLogHandler{}
	reader.RegisterQueryLogger(sql, orm.QueryLoggerOptions{MySQL: true})

	fields := entities.GenerateEntityNoRedisProvider.Fields
	rows, err := entities.GenerateEntityNoRedisProvider.SearchMany(reader, orm.NewQuery().
		Filter(fields.Age.Gte(10)).SortByDESC(fields.Name).Pager(orm.NewPager(1, 2)))
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Equal(t, "c", rows[0].GetName())
	assert.Equal(t, "b", rows[1].GetName())
	assert.Equal(t, uint64(10), rows[0].GetAge(), "all columns are populated from the search query")
	require.Equal(t, 1, countSelects(sql), "rows must not be fetched by a second SELECT")
	query, _ := sql.Logs[0]["query"].(string)
	assert.True(t, strings.HasPrefix(query, "SELECT `ID`,`Age`,"), query)
	assert.Contains(t, query, "ORDER BY `Name` DESC LIMIT 0,2")

	rows, total, err := entities.GenerateEntityNoRedisProvider.SearchManyWithTotal(reader, orm.NewQuery().
		Filter(fields.Age.Gte(10)).SortByASC(fields.Name).Pager(orm.NewPager(2, 2)))
	require.NoError(t, err)
	assert.Equal(t, 3, total)
	require.Len(t, rows, 1)
	assert.Equal(t, "c", rows[0].GetName())
}

// The identity map decides which instance a context hands out. A row read by the direct search path
// must resolve to the instance already in the context, or two copies of one row would drift apart.
func TestSearchWithoutRedisKeepsContextIdentity(t *testing.T) {
	ctx := orm.PrepareTables(t, orm.NewRegistry(), generateEntityNoRedis{}, generateReferenceEntity{})
	saved := newNoRedisRows(t, ctx, "first", "second")

	reader := ctx.Clone()
	fields := entities.GenerateEntityNoRedisProvider.Fields
	byID, err := entities.GenerateEntityNoRedisProvider.GetByID(reader, saved[0].GetID())
	require.NoError(t, err)

	rows, err := entities.GenerateEntityNoRedisProvider.SearchMany(reader, orm.NewQuery().SortByASC(fields.Name))
	require.NoError(t, err)
	require.Len(t, rows, 2)
	assert.Same(t, byID, rows[0], "SearchMany returns the instance the context already holds")

	one, err := entities.GenerateEntityNoRedisProvider.SearchOne(reader, orm.NewQuery().Filter(fields.Name.Is("second")))
	require.NoError(t, err)
	assert.Same(t, rows[1], one, "SearchOne returns the instance SearchMany put in the context")

	again, err := entities.GenerateEntityNoRedisProvider.GetByID(reader, saved[1].GetID())
	require.NoError(t, err)
	assert.Same(t, one, again)
}

// SearchOne without a Redis tier reads the row in its one query and reports a miss as NotFoundError.
func TestSearchOneWithoutRedisReadsRowInOneQuery(t *testing.T) {
	ctx := orm.PrepareTables(t, orm.NewRegistry(), generateEntityNoRedis{}, generateReferenceEntity{})
	newNoRedisRows(t, ctx, "only")

	reader := ctx.Clone()
	reader.DisableContextCache()
	sql := &orm.MockLogHandler{}
	reader.RegisterQueryLogger(sql, orm.QueryLoggerOptions{MySQL: true})

	fields := entities.GenerateEntityNoRedisProvider.Fields
	one, err := entities.GenerateEntityNoRedisProvider.SearchOne(reader, orm.NewQuery().Filter(fields.Name.Is("only")))
	require.NoError(t, err)
	assert.Equal(t, "only", one.GetName())
	assert.Equal(t, uint64(10), one.GetAge())
	assert.Equal(t, 1, countSelects(sql))

	_, err = entities.GenerateEntityNoRedisProvider.SearchOne(reader, orm.NewQuery().Filter(fields.Name.Is("missing")))
	var notFound *orm.NotFoundError
	require.ErrorAs(t, err, &notFound)
}

func mysqlStatusCounter(t *testing.T, ctx orm.Context, name string) int {
	t.Helper()
	var variable string
	var value int
	found, err := ctx.DB(orm.DefaultPoolCode).QueryRow(ctx, orm.NewWhere("SHOW GLOBAL STATUS LIKE ?", name), &variable, &value)
	require.NoError(t, err)
	require.True(t, found)
	return value
}

// A parametrized search must travel in one round trip. With server-side prepared statements the
// driver sends COM_STMT_PREPARE first; the ORM enables client-side interpolation by default so it
// does not. The global counter is shared with other connections, so this only asserts no growth
// attributable to this test's queries on a quiet test server.
func TestParametrizedQueriesAreNotPreparedByDefault(t *testing.T) {
	ctx := orm.PrepareTables(t, orm.NewRegistry(), generateEntityNoRedis{}, generateReferenceEntity{})
	newNoRedisRows(t, ctx, "x")
	fields := entities.GenerateEntityNoRedisProvider.Fields

	// Warm the pool so a fresh connection's handshake does not blur the counter.
	_, err := entities.GenerateEntityNoRedisProvider.SearchMany(ctx, orm.NewQuery().Filter(fields.Name.Is("x")))
	require.NoError(t, err)

	before := mysqlStatusCounter(t, ctx, "Com_stmt_prepare")
	for i := 0; i < 5; i++ {
		rows, err := entities.GenerateEntityNoRedisProvider.SearchMany(ctx, orm.NewQuery().Filter(fields.Name.Is("x")))
		require.NoError(t, err)
		require.Len(t, rows, 1)
	}
	after := mysqlStatusCounter(t, ctx, "Com_stmt_prepare")
	assert.Equal(t, before, after, "parametrized SearchMany must not prepare a server-side statement")
}
