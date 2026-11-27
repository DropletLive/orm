package orm

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"time"
)

// OutboxEntity is the transactional change log. It is declared here rather than
// by the application so the column set is a compile-time fact on both sides:
// orm writes it with raw SQL, the application reads it through the generated
// provider. Register it with `registry.RegisterEntity` and it generates, alters
// and queries like any other entity.
//
// An entity tagged `orm:"outbox"` gets one row per write, inside that write's
// own transaction: the row commits with the change or not at all. Nothing is
// published from it; Debezium owns the change feed. It is a short history for
// audit and debugging: who changed a row, from what, to what, and when.
type OutboxEntity struct {
	ID         uint64    `orm:"table=outbox"`
	EntityName string    `orm:"required;length=100"`
	EntityID   uint64    `orm:"required"`
	Op         uint8     `orm:"required"`
	Payload    string    `orm:"length=max;required"`
	CreatedAt  time.Time `orm:"time"`
}

// Indexes serve the two readers: a row's history (entity, row, time) and the
// purge (time).
func (e OutboxEntity) Indexes() [][]string {
	return [][]string{{"EntityName", "EntityID", "CreatedAt"}, {"CreatedAt"}}
}

const outboxInsertSQL = "INSERT INTO `%s` (`ID`,`EntityName`,`EntityID`,`Op`,`Payload`,`CreatedAt`) VALUES (?,?,?,?,?,?)"

// resolvedOutbox is the registry's handle on the outbox table, resolved once at
// Validate() time from the registered OutboxEntity's schema.
type resolvedOutbox struct {
	tableName string
	poolCode  string
}

// resolveOutbox locates the registered OutboxEntity and checks that every entity
// tagged `outbox` writes to the same MySQL pool. Cross-pool would mean the row
// is not in the source write's transaction at all, which is the entire
// guarantee.
func resolveOutbox(e *engineImplementation) error {
	reg := e.registry
	outboxType := reflect.TypeOf(OutboxEntity{})

	var tagged []string
	for _, schema := range reg.entitySchemas {
		if schema.outbox {
			tagged = append(tagged, schema.t.Name())
		}
	}
	if len(tagged) == 0 {
		return nil
	}
	// Map iteration order is random; sort so the error below names the same
	// entity every run rather than a different one each restart.
	sort.Strings(tagged)

	schema, has := reg.entitySchemas[outboxType]
	if !has {
		return fmt.Errorf(
			"entity '%s' is tagged `orm:\"outbox\"` but orm.OutboxEntity is not registered; add registry.RegisterEntity(orm.OutboxEntity{})",
			tagged[0])
	}
	reg.outbox = &resolvedOutbox{tableName: schema.tableName, poolCode: schema.mysqlPoolCode}

	for _, s := range reg.entitySchemas {
		if !s.outbox {
			continue
		}
		if s.mysqlPoolCode != schema.mysqlPoolCode {
			return fmt.Errorf(
				"entity '%s' is tagged `orm:\"outbox\"` on mysql pool '%s' but the outbox table is on pool '%s'; the outbox row would not be in the same transaction",
				s.t.Name(), s.mysqlPoolCode, schema.mysqlPoolCode)
		}
	}
	return nil
}

// stageOutbox queues one outbox INSERT per written entity tagged `outbox`, onto
// the same database pipeline as the write itself, so the row commits with the
// entity or not at all.
func (orm *ormImplementation) stageOutbox(list []*pendingWrite) error {
	reg := orm.engine.registry
	outbox := reg.outbox
	if outbox == nil {
		return nil
	}

	now := time.Now()
	for _, w := range list {
		eventType, _ := w.entity.PrivateFlushEvent()
		if eventType == 0 {
			continue
		}
		entry, ok := reg.cdcEntityFor(w.entity)
		if !ok || !entry.outbox {
			continue
		}
		op := dirtyOpFromFlushType(eventType)
		payload, err := buildCDCPayload(entry, w.entity, op)
		if err != nil {
			return fmt.Errorf("build outbox payload for %s: %w", entry.entityName, err)
		}
		orm.DatabasePipeLine(outbox.poolCode).AddQueryForTable(
			outbox.tableName,
			fmt.Sprintf(outboxInsertSQL, outbox.tableName),
			orm.engine.NextID(), entry.entityName, w.entity.GetID(), uint8(op), string(payload), now,
		)
	}
	return nil
}

const outboxPurgeSQL = "DELETE FROM `%s` WHERE `CreatedAt` <= ? ORDER BY `ID` LIMIT %d"

// PurgeOutbox deletes up to limit rows older than olderThan and reports how many
// went. Callers loop until it returns fewer than limit.
func PurgeOutbox(ctx Context, olderThan time.Duration, limit int) (int, error) {
	orm, ok := ctx.(*ormImplementation)
	if !ok {
		return 0, errors.New("PurgeOutbox: unsupported context")
	}
	outbox := orm.engine.registry.outbox
	if outbox == nil {
		return 0, nil
	}

	res, err := orm.DB(outbox.poolCode).Exec(orm,
		fmt.Sprintf(outboxPurgeSQL, outbox.tableName, limit), time.Now().Add(-olderThan))
	if err != nil {
		return 0, fmt.Errorf("purge outbox rows: %w", err)
	}
	deleted, err := res.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("purge outbox rows: %w", err)
	}

	return int(deleted), nil
}
