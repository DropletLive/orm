package orm

import (
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// cdcTestRow stands in for a generated entity: a SQL row plus the two methods
// listeners need.
type cdcTestRow struct {
	F0 uint64
	F1 uint64
	F2 sql.NullInt64
	F3 bool
	F4 time.Time
	F5 sql.NullTime
	F6 float64
	F7 sql.NullString
	F8 string
}

var cdcTestColumns = []CDCColumn{
	{Name: "ID", Kind: CDCNumber},
	{Name: "FakeDelete", Kind: CDCNumber},
	{Name: "Product", Kind: CDCNumber},
	{Name: "Active", Kind: CDCBool},
	{Name: "StartsAt", Kind: CDCDatetime},
	{Name: "Birthday", Kind: CDCDate},
	{Name: "Price", Kind: CDCDecimal, Scale: 2},
	{Name: "Note", Kind: CDCString},
	{Name: "Blob", Kind: CDCBytes},
}

func (r *cdcTestRow) fields() []any {
	return []any{&r.F0, &r.F1, &r.F2, &r.F3, &r.F4, &r.F5, &r.F6, &r.F7, &r.F8}
}

func (r *cdcTestRow) GetID() uint64 { return r.F0 }

func (r *cdcTestRow) PrivateColumnValue(column string) any {
	switch column {
	case "FakeDelete":
		return r.F1 != 0
	case "Active":
		return r.F3
	case "Note":
		return r.F7
	}
	return nil
}

func decodeCDCTestRow(_ Context, image map[string]json.RawMessage) (*cdcTestRow, error) {
	row := &cdcTestRow{}
	return row, DecodeCDCRow(cdcTestColumns, row.fields(), image)
}

func cdcTestPayload(t *testing.T, change CDCChange) []byte {
	t.Helper()
	data, err := marshalCDCChange(change, "droplet", "things", time.UnixMilli(1793900000000))
	require.NoError(t, err)
	return data
}

func TestCDCRowRoundTrip(t *testing.T) {
	row := &cdcTestRow{
		F0: 323046535625160701,
		F2: sql.NullInt64{Int64: 323046535625160702, Valid: true},
		F3: true,
		F4: time.Date(2026, 11, 8, 21, 49, 30, 0, time.UTC),
		F5: sql.NullTime{Time: time.Date(1990, 5, 17, 0, 0, 0, 0, time.UTC), Valid: true},
		F6: 12.5,
		F7: sql.NullString{},
		F8: "\x00\x01binary",
	}
	image := EncodeCDCRow(cdcTestColumns, row.fields())
	require.Equal(t, int64(1794174570000), image["StartsAt"])
	require.Equal(t, int64(7441), image["Birthday"])
	require.Equal(t, "12.50", image["Price"])
	require.Nil(t, image["Note"])

	data := cdcTestPayload(t, CDCChange{Op: CDCOpCreate, After: image})
	var env cdcWireEnvelope
	require.NoError(t, json.Unmarshal(data, &env))
	got, err := decodeCDCImage(nil, env.After, decodeCDCTestRow)
	require.NoError(t, err)
	require.Equal(t, row, got)
}

// Debezium's own shapes: booleans as 0/1 without the converter, numbers for
// unsigned bigints, a missing column decoding to its zero value.
func TestDecodeCDCRowDebeziumShapes(t *testing.T) {
	image := map[string]json.RawMessage{
		"ID":       json.RawMessage(`323046535625160701`),
		"Active":   json.RawMessage(`1`),
		"StartsAt": json.RawMessage(`1793915370000`),
		"Price":    json.RawMessage(`"12.50"`),
		"Note":     json.RawMessage(`"hi"`),
	}
	row, err := decodeCDCTestRow(nil, image)
	require.NoError(t, err)
	require.Equal(t, uint64(323046535625160701), row.F0)
	require.True(t, row.F3)
	require.Equal(t, 12.5, row.F6)
	require.Equal(t, sql.NullString{String: "hi", Valid: true}, row.F7)
	require.False(t, row.F5.Valid)
}

func TestDecodeCDCEventOps(t *testing.T) {
	live := EncodeCDCRow(cdcTestColumns, (&cdcTestRow{F0: 7, F3: true}).fields())
	gone := EncodeCDCRow(cdcTestColumns, (&cdcTestRow{F0: 7, F1: 7, F3: true}).fields())
	changed := EncodeCDCRow(cdcTestColumns, (&cdcTestRow{F0: 7, F7: sql.NullString{String: "x", Valid: true}}).fields())

	decode := func(change CDCChange) *DirtyEvent[cdcTestRow] {
		ev, err := decodeCDCEvent[cdcTestRow](nil, cdcTestPayload(t, change), decodeCDCTestRow)
		require.NoError(t, err)
		require.Equal(t, uint64(7), ev.ID)
		require.Equal(t, int64(1793900000000), ev.TsMs)
		return ev
	}

	ev := decode(CDCChange{Op: CDCOpCreate, After: live})
	require.Equal(t, DirtyInsert, ev.Op)
	require.Nil(t, ev.Before)

	ev = decode(CDCChange{Op: CDCOpUpdate, Before: live, After: changed})
	require.Equal(t, DirtyUpdate, ev.Op)
	require.True(t, dirtyEventHasFieldChanges(ev, []string{"Note"}))
	require.False(t, dirtyEventHasFieldChanges(ev, []string{"FakeDelete"}))

	ev = decode(CDCChange{Op: CDCOpUpdate, Before: live, After: gone})
	require.Equal(t, DirtyDelete, ev.Op, "a soft delete reaches listeners as a delete")
	require.Nil(t, ev.After)
	require.NotNil(t, ev.Before)

	ev = decode(CDCChange{Op: CDCOpDelete, Before: live})
	require.Equal(t, DirtyDelete, ev.Op)

	ev = decode(CDCChange{Op: CDCOpRead, After: live})
	require.Equal(t, DirtyUpdate, ev.Op, "a replay is an update with nothing changed")
	require.False(t, dirtyEventHasFieldChanges(ev, []string{"Note", "Active"}))
}

func TestParseCDCIdentity(t *testing.T) {
	op, id, err := parseCDCIdentity([]byte(`{"before":{"ID":323046535625160701},"after":null,"op":"d"}`))
	require.NoError(t, err)
	require.Equal(t, CDCOpDelete, op)
	require.Equal(t, uint64(323046535625160701), id)

	_, _, err = parseCDCIdentity([]byte(`{"before":null,"after":null,"op":"t"}`))
	require.Error(t, err)
}
