package orm

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"
)

// DirtyOp identifies the kind of database mutation that produced an event.
type DirtyOp uint8

const (
	DirtyInsert DirtyOp = 1
	DirtyUpdate DirtyOp = 2
	DirtyDelete DirtyOp = 3
)

// String returns the lowercase name (used in logs and metrics).
func (op DirtyOp) String() string {
	switch op {
	case DirtyInsert:
		return "insert"
	case DirtyUpdate:
		return "update"
	case DirtyDelete:
		return "delete"
	}

	return "unknown"
}

// DirtyEvent is one row change as a listener sees it. Before and After are
// read-only entities decoded from the row images:
//
//	type OrdersDirtyEvent = orm.DirtyEvent[Orders]
//
// A soft delete arrives as DirtyDelete, and a replay as DirtyUpdate with Before
// and After the same row.
type DirtyEvent[T any] struct {
	Op     DirtyOp
	ID     uint64
	Before *T // nil for Insert
	After  *T // nil for Delete
	TsMs   int64
}

// CDCRow is what a generated entity provides so a listener can receive it.
type CDCRow[T any] interface {
	*T
	GetID() uint64
	PrivateColumnValue(column string) any
}

// CDCRowDecoder builds a read-only entity from one row image. Generated.
type CDCRowDecoder[T any] func(ctx Context, row map[string]json.RawMessage) (*T, error)

// HandlerOption configures a single typed handler (field filtering, etc).
type HandlerOption func(*handlerConfig)

type handlerConfig struct {
	watchFields []string
}

// WatchFields skips Update events where none of the listed entity fields differ
// between Before and After. Insert and Delete events always fire. The Field type
// is the same one providers expose (e.g. gen.ShowsProvider.Fields.Status), so
// callers don't need column-name strings.
//
// A replayed event carries identical Before and After, so a handler using
// WatchFields deliberately ignores replays - only full reindexers, which do not
// filter, act on them.
func WatchFields(fields ...Field) HandlerOption {
	names := make([]string, 0, len(fields))
	for _, f := range fields {
		names = append(names, f.ColumnName())
	}

	return func(c *handlerConfig) { c.watchFields = append(c.watchFields, names...) }
}

// BuildDispatch wraps a typed user handler into the generic Dispatch closure.
// Generated OnX methods call this.
func BuildDispatch[T any, PT CDCRow[T]](
	handler func(ctx Context, ev *DirtyEvent[T]) error, decode CDCRowDecoder[T], opts ...HandlerOption,
) Dispatch {
	cfg := &handlerConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	return func(ctx Context, msg *NatsMessage) error {
		ev, err := decodeCDCEvent[T, PT](ctx, msg.Data, decode)
		if err != nil {
			return err
		}
		if !dirtyEventHasFieldChanges[T, PT](ev, cfg.watchFields) {
			return nil
		}

		return handler(ctx, ev)
	}
}

// BuildBatchDispatch wraps a typed user batch handler into the generic
// BatchDispatch closure. Generated OnXBatch methods call this.
//
// Every message in the group is decoded up front and filtered through
// WatchFields; if nothing survives the filter the handler is skipped and the
// group acks. A decode failure fails the whole group rather than silently
// dropping a message.
func BuildBatchDispatch[T any, PT CDCRow[T]](
	handler func(ctx Context, evs []*DirtyEvent[T]) error, decode CDCRowDecoder[T], opts ...HandlerOption,
) BatchDispatch {
	cfg := &handlerConfig{}
	for _, opt := range opts {
		opt(cfg)
	}

	return func(ctx Context, msgs []*NatsMessage) error {
		evs := make([]*DirtyEvent[T], 0, len(msgs))
		for _, msg := range msgs {
			ev, err := decodeCDCEvent[T, PT](ctx, msg.Data, decode)
			if err != nil {
				return err
			}
			if !dirtyEventHasFieldChanges[T, PT](ev, cfg.watchFields) {
				continue
			}
			evs = append(evs, ev)
		}
		if len(evs) == 0 {
			return nil
		}

		return handler(ctx, evs)
	}
}

// decodeCDCEvent turns one envelope into the listener's event. This is the one
// place Debezium's ops become DirtyOps, so the soft delete and replay rules live
// here and nowhere else.
func decodeCDCEvent[T any, PT CDCRow[T]](ctx Context, data []byte, decode CDCRowDecoder[T]) (*DirtyEvent[T], error) {
	var env cdcWireEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return nil, fmt.Errorf("cdc envelope: %w", err)
	}
	before, err := decodeCDCImage(ctx, env.Before, decode)
	if err != nil {
		return nil, fmt.Errorf("cdc before image: %w", err)
	}
	after, err := decodeCDCImage(ctx, env.After, decode)
	if err != nil {
		return nil, fmt.Errorf("cdc after image: %w", err)
	}

	ev := &DirtyEvent[T]{TsMs: env.Source.TsMs, Before: before, After: after}
	if ev.TsMs == 0 {
		ev.TsMs = env.TsMs
	}
	switch env.Op {
	case CDCOpCreate:
		ev.Op = DirtyInsert
		ev.Before = nil
	case CDCOpUpdate:
		ev.Op = DirtyUpdate
		if softDeleted[T, PT](before, after) {
			ev.Op = DirtyDelete
			ev.After = nil
		}
	case CDCOpDelete:
		ev.Op = DirtyDelete
		ev.After = nil
	case CDCOpRead:
		ev.Op = DirtyUpdate
		ev.Before = after
	default:
		return nil, fmt.Errorf("cdc envelope: unknown op %q", env.Op)
	}

	row := ev.After
	if row == nil {
		row = ev.Before
	}
	if row == nil {
		return nil, fmt.Errorf("cdc envelope: op %q carries no row", env.Op)
	}
	ev.ID = PT(row).GetID()

	return ev, nil
}

func decodeCDCImage[T any](ctx Context, raw json.RawMessage, decode CDCRowDecoder[T]) (*T, error) {
	image, err := decodeCDCRowImage(raw)
	if err != nil || image == nil {
		return nil, err
	}

	return decode(ctx, image)
}

// softDeleted reports an update that set FakeDelete. The binlog sees an UPDATE;
// listeners have always seen a delete.
func softDeleted[T any, PT CDCRow[T]](before, after *T) bool {
	if before == nil || after == nil {
		return false
	}
	was, _ := PT(before).PrivateColumnValue("FakeDelete").(bool)
	is, _ := PT(after).PrivateColumnValue("FakeDelete").(bool)

	return !was && is
}

// dirtyEventHasFieldChanges returns true iff at least one of `fields` differs
// between Before and After. Insert, Delete and an empty field list always pass.
func dirtyEventHasFieldChanges[T any, PT CDCRow[T]](ev *DirtyEvent[T], fields []string) bool {
	if len(fields) == 0 || ev.Op != DirtyUpdate || ev.Before == nil || ev.After == nil {
		return true
	}
	before, after := PT(ev.Before), PT(ev.After)
	for _, f := range fields {
		if !reflect.DeepEqual(before.PrivateColumnValue(f), after.PrivateColumnValue(f)) {
			return true
		}
	}

	return false
}

// ----- CDC entity registration -----

// cdcEntity is what generated code adds to an entity that is on the change
// feed: its row image, and the change one write makes.
type cdcEntity interface {
	Entity
	PrivateCDCRow(withChanges bool) map[string]any
	PrivateCDCChange(op DirtyOp) CDCChange
}

// cdcEntityEntry is one table on the change feed.
type cdcEntityEntry struct {
	// subject is cdc.<database>.<table>, known once the engine is validated.
	subject    Subject
	tableName  string
	entityName string
	database   string
	// cdc means listeners read it: the reader forwards its changes.
	cdc         bool
	outbox      bool
	reflectType reflect.Type
}

var cdcEntitiesMu sync.RWMutex
var cdcEntities = make(map[reflect.Type]*cdcEntityEntry)

// RegisterCDCEntity records a generated entity that is on the change feed.
// Called from generated `init()` blocks.
//
// Duplicate registrations for the same E silently replace, so re-running tests
// with hot-reload doesn't error.
func RegisterCDCEntity[E any](table string) {
	var zero E
	t := reflect.TypeOf(zero)
	if t == nil {
		panic("RegisterCDCEntity: E must be a concrete struct type")
	}
	cdcEntitiesMu.Lock()
	cdcEntities[t] = &cdcEntityEntry{
		tableName:   table,
		entityName:  t.Name(),
		reflectType: t,
	}
	cdcEntitiesMu.Unlock()
}

// getCDCEntityByName returns the entry whose generated entity struct has the
// given Go name. The schema's reflect.Type points at the user-supplied source
// struct (e.g. ProductEntity) while init() registers the generated struct (e.g.
// Products) - distinct types in distinct packages, so a reflect.Type lookup
// would always miss.
func getCDCEntityByName(name string) (*cdcEntityEntry, bool) {
	cdcEntitiesMu.RLock()
	defer cdcEntitiesMu.RUnlock()
	for _, entry := range cdcEntities {
		if entry.entityName == name {
			return entry, true
		}
	}

	return nil, false
}

// generatedEntityName produces the Go identifier for the entity struct that
// orm.Generate emits from a schema's tableName. Mirrors codeGenerator's
// capitalizeFirst (split on `_`, capitalize each part's first letter) so the
// lookup at Validate() time matches the name the generator used.
func generatedEntityName(tableName string) string {
	parts := strings.Split(tableName, "_")
	for i, part := range parts {
		if part == "" {
			continue
		}
		b := []byte(part)
		if b[0] >= 'a' && b[0] <= 'z' {
			b[0] = b[0] - ('a' - 'A')
		}
		parts[i] = string(b)
	}

	return strings.Join(parts, "")
}

// isOnCDCFeed decides which tables get a row image codec: the ones listeners
// declare, and the outbox ones whose row stores the same payload.
func isOnCDCFeed(schema *entitySchema) bool {
	return schema.cdc || schema.outbox
}

// resolveCDCEntities runs at Validate() time. It copies each registered entry
// rather than writing to it: the registry is package-global and shared by every
// engine in the process.
//
// A missing entry is tolerated: TestGenerate runs Validate before the freshly
// generated init() blocks are compiled in.
func resolveCDCEntities(e *engineImplementation) {
	reg := e.registry
	reg.cdcEntities = make(map[reflect.Type]*cdcEntityEntry)

	for _, schema := range reg.entitySchemas {
		if !isOnCDCFeed(schema) {
			continue
		}
		entry, ok := getCDCEntityByName(generatedEntityName(schema.tableName))
		if !ok {
			continue
		}
		scoped := *entry
		scoped.cdc = schema.cdc
		scoped.outbox = schema.outbox
		scoped.database = e.dbServers[schema.mysqlPoolCode].GetConfig().GetDatabaseName()
		scoped.subject = CDCSubject(scoped.database, schema.tableName)
		reg.cdcEntities[entry.reflectType] = &scoped
	}
}

func (reg *engineRegistryImplementation) cdcEntityFor(entity Entity) (*cdcEntityEntry, bool) {
	t := reflect.TypeOf(entity)
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	entry, ok := reg.cdcEntities[t]

	return entry, ok
}

// ----- Publish -----

// newEntityMessage builds the message every publish path uses - the post-commit
// flush, the outbox relay, the reader's forward and replay - including the
// deterministic Nats-Msg-Id that JetStream dedups on. One builder, so a relayed
// message can never drift from the original and lose its dedup.
func newEntityMessage(subject Subject, payload []byte) *NatsMessage {
	msg := NewNatsMessage(string(subject))
	msg.Data = payload
	msg.Headers.Set("Nats-Msg-Id", fmt.Sprintf("%s:%d", subject, msgIDHash(payload)))

	return msg
}

// dirtyOpFromFlushType maps the entity's internal flush-type code (1/2/3) to DirtyOp.
func dirtyOpFromFlushType(t uint8) DirtyOp {
	switch t {
	case 1:
		return DirtyInsert
	case 2:
		return DirtyUpdate
	case 3:
		return DirtyDelete
	}

	return 0
}

// buildCDCPayload serialises one write's change. The envelope carries the
// commit time, so the bytes are not deterministic: callers that need the same
// bytes twice (the outbox) keep the first serialisation.
func buildCDCPayload(entry *cdcEntityEntry, entity Entity, op DirtyOp) ([]byte, error) {
	source, ok := entity.(cdcEntity)
	if !ok {
		return nil, fmt.Errorf("%w: %T", ErrEntityNeedsRegeneration, entity)
	}

	return marshalCDCChange(source.PrivateCDCChange(op), entry.database, entry.tableName, time.Now())
}

// pendingDirtyMessages accumulates messages across a whole Flush, keyed by NATS
// pool, so each pool is published in one batch instead of one round-trip per
// entity.
type pendingDirtyMessages map[string][]*NatsMessage

// buildDirtyEvent serialises the event once and appends exactly one message,
// whatever number of consumers read that entity.
func buildDirtyEvent(orm Context, pending pendingDirtyMessages, entry *cdcEntityEntry, w *pendingWrite, op DirtyOp) error {
	// Outbox staging already serialised this event. Reuse those exact bytes, so
	// the relay replays it under the same dedup id.
	payload := w.dirtyPayload
	if payload == nil {
		var err error
		payload, err = buildCDCPayload(entry, w.entity, op)
		if err != nil {
			return fmt.Errorf("build cdc event for %s: %w", entry.entityName, err)
		}
	}

	poolCode := orm.Engine().Registry().(*engineRegistryImplementation).cdcStream.NatsPool
	if pool := orm.Engine().Nats(poolCode); pool == nil {
		return fmt.Errorf("nats pool '%s' for the cdc stream not configured", poolCode)
	}
	pending[poolCode] = append(pending[poolCode], newEntityMessage(entry.subject, payload))

	return nil
}

// flushDirtyMessages publishes each pool's accumulated messages in one batch.
// Called at the end of the Flush entity walk; Flush still waits for the acks, so
// "flush succeeded ⇒ event durably published" continues to hold.
func flushDirtyMessages(orm Context, pending pendingDirtyMessages) error {
	for poolCode, msgs := range pending {
		pool := orm.Engine().Nats(poolCode)
		if pool == nil {
			return fmt.Errorf("nats pool '%s' for entity publish not configured", poolCode)
		}
		if err := pool.PublishBatch(orm, msgs); err != nil {
			return fmt.Errorf("publish %d entity events to pool %s: %w", len(msgs), poolCode, err)
		}
	}

	return nil
}

// msgIDHash hashes the payload bytes into a uint64 for use in Nats-Msg-Id.
// Combined with the subject it scopes the dedup window correctly: two retries
// of the same event share an id, but unrelated events don't collide.
func msgIDHash(b []byte) uint64 {
	var h uint64 = 14695981039346656037
	for _, c := range b {
		h ^= uint64(c)
		h *= 1099511628211
	}

	return h
}
