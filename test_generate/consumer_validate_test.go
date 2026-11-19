package test_generate

import (
	"testing"

	"github.com/DropletLive/orm"
	"github.com/stretchr/testify/assert"
)

// The structs below exist only to drive Validate. They are not in
// FixtureEntities, so they have no generated code and never reach a query.

type oldDirtyTagEntity struct {
	ID   uint64 `orm:"dirty=some-stream"`
	Name string `orm:"required;length=100"`
}

type consumedCdcEntity struct {
	ID   uint64
	Name string `orm:"required;length=100"`
}

type unregisteredEntity struct {
	ID   uint64
	Name string `orm:"required;length=100"`
}

func validateRegistry(entities []any, consumers ...orm.ConsumerDef) error {
	registry := orm.NewRegistry()
	registry.RegisterMySQL("root:root@tcp(localhost:3397)/test", "default", &orm.MySQLOptions{})
	registry.RegisterRedis("localhost:6395", 0, "default", nil)
	registry.RegisterEntity(entities...)
	for _, def := range consumers {
		registry.RegisterConsumer(def)
	}
	_, err := registry.Validate()

	return err
}

// TestValidateRejectsTheOldDirtyTag stops an entity being half-migrated. The
// tag used to name the entity's readers; leaving it accepted-but-ignored would
// mean an entity that silently stopped publishing.
func TestValidateRejectsTheOldDirtyTag(t *testing.T) {
	err := validateRegistry([]any{oldDirtyTagEntity{}})

	assert.ErrorContains(t, err, "no longer exists")
	assert.ErrorContains(t, err, "oldDirtyTagEntity")
	assert.ErrorContains(t, err, "ConsumerDef")
}

func TestValidateRejectsAConsumerDeclaringAnUnregisteredEntity(t *testing.T) {
	err := validateRegistry(
		[]any{consumedCdcEntity{}},
		orm.ConsumerDef{Name: "reader", Entities: []any{consumedCdcEntity{}, unregisteredEntity{}}},
	)

	assert.ErrorContains(t, err, "is not registered")
	assert.ErrorContains(t, err, "unregisteredEntity")
}

func TestValidateRejectsADuplicateConsumerName(t *testing.T) {
	err := validateRegistry(
		[]any{consumedCdcEntity{}},
		orm.ConsumerDef{Name: "reader", Entities: []any{consumedCdcEntity{}}},
		orm.ConsumerDef{Name: "reader", Entities: []any{consumedCdcEntity{}}},
	)

	assert.ErrorContains(t, err, "declared twice")
}

// TestValidateRejectsAConsumerWithOverlappingFilters catches at boot what
// JetStream would otherwise reject at reconcile time, when the error is a long
// way from the declaration that caused it.
func TestValidateRejectsAConsumerWithOverlappingFilters(t *testing.T) {
	err := validateRegistry(
		[]any{consumedCdcEntity{}},
		orm.ConsumerDef{Name: "reader", Entities: []any{consumedCdcEntity{}, consumedCdcEntity{}}},
	)

	assert.ErrorContains(t, err, "twice")
	assert.ErrorContains(t, err, "overlap")
}

func TestValidateRejectsABadConsumerName(t *testing.T) {
	err := validateRegistry(
		[]any{consumedCdcEntity{}},
		orm.ConsumerDef{Name: "Order_Indexer", Entities: []any{consumedCdcEntity{}}},
	)

	assert.ErrorContains(t, err, "must match")
}

func TestValidateRejectsAConsumerWithNoEntities(t *testing.T) {
	err := validateRegistry(
		[]any{consumedCdcEntity{}},
		orm.ConsumerDef{Name: "reader", Entities: []any{consumedCdcEntity{}}},
		orm.ConsumerDef{Name: "idle"},
	)

	assert.ErrorContains(t, err, "declares neither entities nor queues")
}

func TestValidateAcceptsAnEntityOnSeveralConsumers(t *testing.T) {
	err := validateRegistry(
		[]any{consumedCdcEntity{}},
		orm.ConsumerDef{Name: "reader-one", Entities: []any{consumedCdcEntity{}}},
		orm.ConsumerDef{Name: "reader-two", Entities: []any{consumedCdcEntity{}}},
	)

	assert.NoError(t, err, "several consumers reading one entity is the whole point of subject filters")
}
