package elasticsearch

import (
	"time"
)

type Condition interface {
	Build() map[string]any
}

type Field interface {
	FieldName() string
}

type UintField string

func (f UintField) FieldName() string { return string(f) }

func (f UintField) In(values ...uint64) Condition {
	if len(values) == 0 {
		return nil
	}

	return termsCond{field: string(f), values: values}
}

func (f UintField) Eq(value uint64) Condition {
	return termCond{field: string(f), value: value}
}

func (f UintField) Gte(value uint64) Condition {
	if value == 0 {
		return nil
	}

	return rangeCond{field: string(f), gte: value}
}

func (f UintField) Lte(value uint64) Condition {
	if value == 0 {
		return nil
	}

	return rangeCond{field: string(f), lte: value}
}

func (f UintField) Between(gte, lte uint64) Condition {
	if gte == 0 && lte == 0 {
		return nil
	}

	c := rangeCond{field: string(f)}

	if gte > 0 {
		c.gte = gte
	}

	if lte > 0 {
		c.lte = lte
	}

	return c
}

type IntField string

func (f IntField) FieldName() string { return string(f) }

func (f IntField) Eq(value int) Condition {
	return termCond{field: string(f), value: value}
}

func (f IntField) Gte(value int) Condition {
	return rangeCond{field: string(f), gte: value}
}

func (f IntField) Lte(value int) Condition {
	return rangeCond{field: string(f), lte: value}
}

type StringField string

func (f StringField) FieldName() string { return string(f) }

func (f StringField) In(values ...string) Condition {
	if len(values) == 0 {
		return nil
	}

	return termsCond{field: string(f), values: values}
}

func (f StringField) Eq(value string) Condition {
	if value == "" {
		return nil
	}

	return termCond{field: string(f), value: value}
}

type EnumField[T ~string] string

func (f EnumField[T]) FieldName() string { return string(f) }

func (f EnumField[T]) In(values ...T) Condition {
	if len(values) == 0 {
		return nil
	}

	strs := make([]string, 0, len(values))
	for _, v := range values {
		if s := string(v); s != "" {
			strs = append(strs, s)
		}
	}

	if len(strs) == 0 {
		return nil
	}

	return termsCond{field: string(f), values: strs}
}

func (f EnumField[T]) Eq(value T) Condition {
	s := string(value)
	if s == "" {
		return nil
	}

	return termCond{field: string(f), value: s}
}

type BoolField string

func (f BoolField) FieldName() string { return string(f) }

func (f BoolField) Is(value bool) Condition {
	return termCond{field: string(f), value: value}
}

type TextField string

func (f TextField) FieldName() string { return string(f) }

func (f TextField) Match(query string, boost float64) Condition {
	if query == "" {
		return nil
	}

	clause := map[string]any{"query": query, "operator": "and"}
	if boost != 0 {
		clause["boost"] = boost
	}

	return rawCond{dsl: map[string]any{"match": map[string]any{string(f): clause}}}
}

func (f TextField) MatchFuzzy(query string, boost float64, fuzziness string, prefixLen int) Condition {
	if query == "" {
		return nil
	}

	clause := map[string]any{
		"query":         query,
		"operator":      "and",
		"fuzziness":     fuzziness,
		"prefix_length": prefixLen,
	}

	if boost != 0 {
		clause["boost"] = boost
	}

	return rawCond{dsl: map[string]any{"match": map[string]any{string(f): clause}}}
}

func (f TextField) Prefix(value string) Condition {
	if value == "" {
		return nil
	}

	return rawCond{dsl: map[string]any{"prefix": map[string]any{string(f): map[string]any{"value": value}}}}
}

func MultiMatch(query, matchType string, fields []string, fuzziness string) Condition {
	if query == "" {
		return nil
	}

	mm := map[string]any{
		"query":    query,
		"type":     matchType,
		"fields":   fields,
		"operator": "and",
	}

	if fuzziness != "" {
		mm["fuzziness"] = fuzziness
	}

	return rawCond{dsl: map[string]any{"multi_match": mm}}
}

func (f TextField) Autocomplete() TextField {
	return TextField(string(f) + ".autocomplete")
}

func (f TextField) Stemmed() TextField {
	return TextField(string(f) + ".stemmed")
}

type DateField string

func (f DateField) FieldName() string { return string(f) }

func (f DateField) Gte(value time.Time) Condition {
	if value.IsZero() {
		return nil
	}

	return rangeCond{field: string(f), gte: value}
}

func (f DateField) Lte(value time.Time) Condition {
	if value.IsZero() {
		return nil
	}

	return rangeCond{field: string(f), lte: value}
}

type FloatField string

func (f FloatField) FieldName() string { return string(f) }

func (f FloatField) Gte(value float64) Condition {
	return rangeCond{field: string(f), gte: value}
}

func (f FloatField) Lte(value float64) Condition {
	return rangeCond{field: string(f), lte: value}
}

func RawCondition(dsl map[string]any) Condition {
	return rawCond{dsl: dsl}
}

func All(conditions ...Condition) Condition {
	filters := make([]map[string]any, 0, len(conditions))

	for _, c := range conditions {
		if c != nil {
			filters = append(filters, c.Build())
		}
	}

	if len(filters) == 0 {
		return nil
	}

	return rawCond{dsl: map[string]any{
		"bool": map[string]any{"filter": filters},
	}}
}

func Not(condition Condition) Condition {
	if condition == nil {
		return nil
	}

	return rawCond{dsl: map[string]any{
		"bool": map[string]any{"must_not": []map[string]any{condition.Build()}},
	}}
}

func Any(conditions ...Condition) Condition {
	shoulds := make([]map[string]any, 0, len(conditions))

	for _, c := range conditions {
		if c != nil {
			shoulds = append(shoulds, c.Build())
		}
	}

	if len(shoulds) == 0 {
		return nil
	}

	if len(shoulds) == 1 {
		return rawCond{dsl: shoulds[0]}
	}

	return rawCond{dsl: map[string]any{
		"bool": map[string]any{
			"should":               shoulds,
			"minimum_should_match": 1,
		},
	}}
}

type termCond struct {
	field string
	value any
}

func (c termCond) Build() map[string]any {
	return map[string]any{"term": map[string]any{c.field: c.value}}
}

type termsCond struct {
	field  string
	values any
}

func (c termsCond) Build() map[string]any {
	return map[string]any{"terms": map[string]any{c.field: c.values}}
}

type rangeCond struct {
	field string
	gte   any
	lte   any
}

func (c rangeCond) Build() map[string]any {
	r := map[string]any{}

	if c.gte != nil {
		r["gte"] = c.gte
	}

	if c.lte != nil {
		r["lte"] = c.lte
	}

	return map[string]any{"range": map[string]any{c.field: r}}
}

type rawCond struct {
	dsl map[string]any
}

func (c rawCond) Build() map[string]any {
	return c.dsl
}
