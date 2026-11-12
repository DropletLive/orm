package elasticsearch

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func rawResultWithIDs(ids ...uint64) *RawResult {
	r := &RawResult{}
	for _, id := range ids {
		r.Hits.HitsList = append(r.Hits.HitsList, RawHit{
			Source: json.RawMessage(`{"id":` + jsonUint(id) + `}`),
		})
	}

	return r
}

func jsonUint(id uint64) string {
	b, _ := json.Marshal(id)

	return string(b)
}

func TestParseHitsPreservesLargeSnowflakeIDs(t *testing.T) {
	ids := []uint64{320141976362082304, 320141976357888000}

	hits := rawResultWithIDs(ids...).ParseHits()

	assert.Len(t, hits, 2)
	assert.Equal(t, ids[0], hits[0].ID)
	assert.Equal(t, ids[1], hits[1].ID)
	assert.NotEqual(t, hits[0].ID, hits[1].ID, "distinct snowflake IDs must not collapse")
}

func TestAsUint64PreservesSnowflakeFromJSONNumber(t *testing.T) {
	const id = uint64(323046535625160701)

	var v any

	dec := json.NewDecoder(strings.NewReader(jsonUint(id)))
	dec.UseNumber()
	assert.NoError(t, dec.Decode(&v))
	assert.IsType(t, json.Number(""), v)

	got, ok := AsUint64(v)
	assert.True(t, ok)
	assert.Equal(t, id, got)

	rounded, _ := AsUint64(float64(id))
	assert.NotEqual(t, id, rounded, "float64 input rounds - helper relies on json.Number for exactness")
}

func TestAsUint64AndAsFloat64Variants(t *testing.T) {
	u, ok := AsUint64(json.Number("42"))
	assert.True(t, ok)
	assert.Equal(t, uint64(42), u)

	u, ok = AsUint64(float64(42))
	assert.True(t, ok)
	assert.Equal(t, uint64(42), u)

	_, ok = AsUint64("not a number")
	assert.False(t, ok)

	f, ok := AsFloat64(json.Number("3.5"))
	assert.True(t, ok)
	assert.Equal(t, 3.5, f)

	f, ok = AsFloat64(3.5)
	assert.True(t, ok)
	assert.Equal(t, 3.5, f)

	_, ok = AsFloat64("not a number")
	assert.False(t, ok)
}
