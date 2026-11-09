//go:build cdc_debezium

package test_generate

// Run with Debezium up (docker compose up debezium):
// go test -tags cdc_debezium -run TestORMFeedMatchesDebezium ./test_generate/

import (
	"bytes"
	"context"
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/DropletLive/orm"
	"github.com/DropletLive/orm/test_generate/entities"
	"github.com/DropletLive/orm/test_generate/entities/enums"
	"github.com/DropletLive/orm/test_generate/models"
)

const (
	parityORMSubject = "cdc.test.generateEntityParity"
	parityDBZSubject = "dbz.test.generateEntityParity"
)

type parityEnvelope struct {
	Op     string
	Before map[string]any
	After  map[string]any
}

// TestORMFeedMatchesDebezium is what makes switching the producer a non-event:
// every row image the ORM publishes must equal the one Debezium reads from the
// binlog for the same write, value for value.
func TestORMFeedMatchesDebezium(t *testing.T) {
	ctx := orm.PrepareTablesWithConsumers(t, orm.NewRegistry(), []orm.ConsumerDef{FixtureParityConsumer()},
		generateEntityParity{}, generateReferenceEntity{})
	defer ctx.Engine().Nats("nats").Close()

	js, err := ctx.Engine().Nats("nats").GetJetStream()
	require.NoError(t, err)
	dbzStream := waitForDebezium(t, ctx, js)

	ormCons := newParityConsumer(t, js, orm.CDCStreamName, parityORMSubject)
	dbzCons := newParityConsumer(t, js, dbzStream, parityDBZSubject)

	ref := entities.GenerateReferenceEntityProvider.New(ctx)
	ref.SetName("ref")
	require.NoError(t, ctx.Save(ref))

	at := time.Date(2026, 11, 8, 21, 49, 30, 0, time.UTC)
	day := time.Date(1990, 5, 17, 0, 0, 0, 0, time.UTC)
	age, balance, flag, ratio := uint64(7), int64(-3), true, 2.25
	enum := enums.TestEnumList.B

	e := entities.GenerateEntityParityProvider.New(ctx)
	e.SetAge(42).SetBalance(-8).SetName("parity").SetComment("comment").
		SetTestEnum(enums.TestEnumList.A).SetTestSet(enums.TestEnumList.A, enums.TestEnumList.C).
		SetByte([]uint8{0, 1, 2, 250}).SetBool(true).SetFloat(1.5).SetPrice(12.5).
		SetTime(at).SetDate(day).SetReferenceRequired(ref.GetID()).SetTagsIDs([]uint64{ref.GetID()}).
		SetSize(3).SetJsonAddress(&models.GenerateJsonAddress{Street: "Main", City: "Wro", Zip: "50"})
	require.NoError(t, ctx.Save(e))

	e.SetAgeNullable(&age).SetBalanceNullable(&balance).SetTestEnumOptional(&enum).
		SetTestSetOptional(enums.TestEnumList.B).SetBoolNullable(&flag).SetFloatNullable(&ratio).
		SetTimeNullable(&at).SetDateNullable(&day).SetReferenceOptional(ref.GetID()).SetPrice(99.99)
	require.NoError(t, ctx.Save(e))

	e.SetAgeNullable(nil).SetBalanceNullable(nil).SetTestEnumOptional(nil).SetTestSetOptional().
		SetBoolNullable(nil).SetFloatNullable(nil).SetTimeNullable(nil).SetDateNullable(nil).
		SetReferenceOptional(0).SetComment("").SetJsonAddress(nil).SetByte(nil)
	require.NoError(t, ctx.Save(e))

	require.NoError(t, ctx.Delete(e))
	require.NoError(t, ctx.ForceDelete(e))

	const changes = 5
	fromORM := fetchParity(t, ormCons, changes)
	fromDBZ := fetchParity(t, dbzCons, changes)
	require.Len(t, fromDBZ, changes, "Debezium did not see every write")
	for i := range changes {
		assert.Equal(t, fromDBZ[i].Op, fromORM[i].Op, "change %d op", i)
		assert.Equal(t, fromDBZ[i].Before, fromORM[i].Before, "change %d before image", i)
		assert.Equal(t, fromDBZ[i].After, fromORM[i].After, "change %d after image", i)
	}
}

// waitForDebezium writes probe rows until Debezium publishes one, so the
// scenario starts with Debezium streaming past it.
func waitForDebezium(t *testing.T, ctx orm.Context, js jetstream.JetStream) string {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		probe := entities.GenerateEntityParityProvider.New(ctx)
		probe.SetName("probe").SetTestEnum(enums.TestEnumList.A).SetTestSet(enums.TestEnumList.A).SetTime(time.Now()).SetDate(time.Now())
		require.NoError(t, ctx.Save(probe))
		require.NoError(t, ctx.ForceDelete(probe))

		stream, err := js.StreamNameBySubject(ctx.Context(), parityDBZSubject)
		if err == nil {
			info, infoErr := js.Stream(ctx.Context(), stream)
			if infoErr == nil {
				if state, sErr := info.Info(ctx.Context()); sErr == nil && state.State.Msgs > 0 {
					return stream
				}
			}
		}
		time.Sleep(time.Second)
	}
	t.Fatal("Debezium never published; is the debezium compose service up?")

	return ""
}

// newParityConsumer starts right after the stream's current last message, so a
// slow producer cannot make it skip anything.
func newParityConsumer(t *testing.T, js jetstream.JetStream, stream, subject string) jetstream.Consumer {
	t.Helper()
	ctx := context.Background()
	s, err := js.Stream(ctx, stream)
	require.NoError(t, err)
	info, err := s.Info(ctx)
	require.NoError(t, err)
	cons, err := s.CreateConsumer(ctx, jetstream.ConsumerConfig{
		FilterSubject: subject,
		DeliverPolicy: jetstream.DeliverByStartSequencePolicy,
		OptStartSeq:   info.State.LastSeq + 1,
		AckPolicy:     jetstream.AckNonePolicy,
	})
	require.NoError(t, err)

	return cons
}

func fetchParity(t *testing.T, cons jetstream.Consumer, n int) []parityEnvelope {
	t.Helper()
	var out []parityEnvelope
	deadline := time.Now().Add(60 * time.Second)
	for len(out) < n && time.Now().Before(deadline) {
		batch, err := cons.Fetch(n-len(out), jetstream.FetchMaxWait(2*time.Second))
		require.NoError(t, err)
		for msg := range batch.Messages() {
			if env := decodeParity(t, msg.Data()); !isProbe(env) {
				out = append(out, env)
			}
		}
	}

	return out
}

// isProbe drops waitForDebezium's rows, which Debezium may still be publishing.
func isProbe(env parityEnvelope) bool {
	row := env.After
	if row == nil {
		row = env.Before
	}

	return row["Name"] == "probe"
}

func decodeParity(t *testing.T, data []byte) parityEnvelope {
	t.Helper()
	var raw struct {
		Op     string         `json:"op"`
		Before map[string]any `json:"before"`
		After  map[string]any `json:"after"`
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	require.NoError(t, dec.Decode(&raw))

	return parityEnvelope{Op: raw.Op, Before: normalizeParity(raw.Before), After: normalizeParity(raw.After)}
}

// normalizeParity compares numbers by value, so 1.5 and 1.50 agree.
func normalizeParity(row map[string]any) map[string]any {
	if row == nil {
		return nil
	}
	out := make(map[string]any, len(row))
	for k, v := range row {
		if n, ok := v.(json.Number); ok {
			if i, err := strconv.ParseInt(string(n), 10, 64); err == nil {
				out[k] = i
				continue
			}
			f, _ := n.Float64()
			out[k] = f
			continue
		}
		out[k] = v
	}

	return out
}
