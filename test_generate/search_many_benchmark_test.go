package test_generate

import (
	"context"
	"strings"
	"testing"

	"github.com/DropletLive/orm"
	"github.com/DropletLive/orm/test_generate/entities"
)

var (
	searchMany10Sink        []*entities.GenerateEntity
	searchMany10NoRedisSink []*entities.GenerateEntityNoRedis
)

// BenchmarkSearchMany10 measures one SearchMany call returning the ten fixture
// rows: filtered by ReferenceRequired, ordered by Age and paged to ten results.
// It shares the populated 27-column fixtures with BenchmarkGetByIDs10. For the
// Redis-cached entity the difference between the two benchmarks is the cost of
// building and running the ID query; the entity without Redis reads its rows in
// the search query itself. The DBQuery is built once outside the timer; context
// creation, getters and query-path checks are outside timing too.
func BenchmarkSearchMany10(b *testing.B) {
	engine, ids, referenceID := prepareGetByIDs10(b)
	b.Run("ContextCacheHit", func(b *testing.B) {
		fields := entities.GenerateEntityProvider.Fields
		query := orm.NewQuery().Filter(fields.ReferenceRequired.Eq(referenceID)).
			SortByASC(fields.Age).Pager(orm.NewPager(1, len(ids)))
		benchmarkSearchMany10(b, engine, ids, referenceID, false, 1, 0,
			entities.GenerateEntityProvider.SearchMany, query, &searchMany10Sink)
	})
	b.Run("RedisCacheHit", func(b *testing.B) {
		fields := entities.GenerateEntityProvider.Fields
		query := orm.NewQuery().Filter(fields.ReferenceRequired.Eq(referenceID)).
			SortByASC(fields.Age).Pager(orm.NewPager(1, len(ids)))
		benchmarkSearchMany10(b, engine, ids, referenceID, true, 1, 1,
			entities.GenerateEntityProvider.SearchMany, query, &searchMany10Sink)
	})
	b.Run("MySQL", func(b *testing.B) {
		fields := entities.GenerateEntityNoRedisProvider.Fields
		query := orm.NewQuery().Filter(fields.ReferenceRequired.Eq(referenceID)).
			SortByASC(fields.Age).Pager(orm.NewPager(1, len(ids)))
		// Without a Redis tier the rows come from the search query itself: one SELECT.
		benchmarkSearchMany10(b, engine, ids, referenceID, true, 1, 0,
			entities.GenerateEntityNoRedisProvider.SearchMany, query, &searchMany10NoRedisSink)
	})
}

func benchmarkSearchMany10[T getByIDsBenchmarkEntity](b *testing.B, engine orm.Engine,
	ids []uint64, referenceID uint64, disableContextCache bool, wantSQL, wantRedis int,
	search func(orm.Context, *orm.DBQuery) ([]T, error), query *orm.DBQuery, sink *[]T,
) {
	ctx := engine.NewContext(context.Background())
	if disableContextCache {
		ctx.DisableContextCache()
	}
	// Populate caches and open connections before the measured steady-state reads.
	rows, err := search(ctx, query)
	if err != nil {
		b.Fatalf("warm SearchMany: err=%v", err)
	}
	checkGetByIDs10(b, rows, ids, referenceID)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		*sink, err = search(ctx, query)
		if err != nil {
			b.Fatalf("SearchMany: err=%v", err)
		}
	}
	b.StopTimer()
	checkGetByIDs10(b, *sink, ids, referenceID)

	// Attach loggers only after stopping the timer: they allocate per query.
	// Probe the actual measured context to catch an unintended cache/fallback path.
	sqlLog, redisLog := &orm.MockLogHandler{}, &orm.MockLogHandler{}
	ctx.RegisterQueryLogger(sqlLog, orm.QueryLoggerOptions{MySQL: true})
	ctx.RegisterQueryLogger(redisLog, orm.QueryLoggerOptions{Redis: true})
	rows, err = search(ctx, query)
	if err != nil {
		b.Fatalf("verification SearchMany: err=%v", err)
	}
	checkGetByIDs10(b, rows, ids, referenceID)
	if countSelects(sqlLog) != wantSQL || len(redisLog.Logs) != wantRedis {
		b.Fatalf("wrong read path: got %d SELECTs and %d Redis operations; want %d and %d",
			countSelects(sqlLog), len(redisLog.Logs), wantSQL, wantRedis)
	}
	// The logger appends bound parameters after the SQL text. Redis-cached
	// entities select IDs only; others select the full rows in the same query.
	searchQuery, _ := sqlLog.Logs[0]["query"].(string)
	if !strings.HasPrefix(searchQuery, "SELECT `ID`") ||
		!strings.Contains(searchQuery, "WHERE `ReferenceRequired` = ? ORDER BY `Age` ASC LIMIT 0,10") {
		b.Fatalf("unexpected search query: %q", searchQuery)
	}
	if wantRedis == 1 {
		redisQuery, _ := redisLog.Logs[0]["query"].(string)
		if strings.Count(strings.ToUpper(redisQuery), "LRANGE ") != len(ids) {
			b.Fatalf("expected one Redis pipeline with %d LRANGE commands: %q", len(ids), redisQuery)
		}
	}
	*sink = nil
}
