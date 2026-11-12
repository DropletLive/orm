package elasticsearch

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func matchClause(t *testing.T, dsl map[string]any, field string) map[string]any {
	t.Helper()

	match, ok := dsl["match"].(map[string]any)
	assert.True(t, ok, "expected a match clause")

	clause, ok := match[field].(map[string]any)
	assert.True(t, ok, "expected clause for field %s", field)

	return clause
}

func TestTextFieldMatchUsesAndOperator(t *testing.T) {
	clause := matchClause(t, TextField("title").Match("ed hardy", 2.0).Build(), "title")

	assert.Equal(t, "and", clause["operator"])
	assert.Equal(t, "ed hardy", clause["query"])
	assert.Equal(t, 2.0, clause["boost"])

	assert.Nil(t, TextField("title").Match("", 1.0))
}

func TestTextFieldMatchFuzzyUsesAndOperator(t *testing.T) {
	clause := matchClause(t, TextField("title.fuzzy").MatchFuzzy("ed hardy", 1.0, "AUTO", 2).Build(), "title.fuzzy")

	assert.Equal(t, "and", clause["operator"])
	assert.Equal(t, "AUTO", clause["fuzziness"])
	assert.Equal(t, 2, clause["prefix_length"])

	assert.Nil(t, TextField("title.fuzzy").MatchFuzzy("", 1.0, "AUTO", 2))
}

func TestMultiMatchUsesAndOperator(t *testing.T) {
	q := NewQuery().MustAll(MultiMatch("ed hardy", "best_fields", []string{"title.autocomplete^3"}, "AUTO"))

	must, ok := q.Build()["query"].(map[string]any)["bool"].(map[string]any)["must"].([]map[string]any)
	assert.True(t, ok)
	assert.Len(t, must, 1)

	mm, ok := must[0]["multi_match"].(map[string]any)
	assert.True(t, ok)
	assert.Equal(t, "and", mm["operator"])
	assert.Equal(t, "best_fields", mm["type"])
	assert.Equal(t, "AUTO", mm["fuzziness"])

	assert.Nil(t, MultiMatch("", "best_fields", []string{"title"}, ""))
}

func TestShouldMatchClausesUseAndOperator(t *testing.T) {
	q := NewQuery().
		ShouldMatch("title", "ed hardy", 3.0).
		ShouldMatchFuzzy("title.fuzzy", "ed hardy", 1.0, "AUTO", 2).
		MinimumShouldMatch(1)

	boolQuery, ok := q.Build()["query"].(map[string]any)["bool"].(map[string]any)
	assert.True(t, ok)

	should, ok := boolQuery["should"].([]map[string]any)
	assert.True(t, ok)
	assert.Len(t, should, 2)

	assert.Equal(t, "and", matchClause(t, should[0], "title")["operator"])
	assert.Equal(t, "and", matchClause(t, should[1], "title.fuzzy")["operator"])

	assert.Equal(t, 1, boolQuery["minimum_should_match"])
}
