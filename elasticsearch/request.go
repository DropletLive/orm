package elasticsearch

import "time"

type fnScoreOpts struct {
	functions []map[string]any
	scoreMode string
	boostMode string
}

type QueryBuilder struct {
	must          []map[string]any
	filter        []map[string]any
	mustNot       []map[string]any
	should        []map[string]any
	minShould     int
	size          int
	from          int
	source        []string
	sort          []map[string]any
	postFilter    []map[string]any
	aggs          map[string]any
	fnScore       *fnScoreOpts
	collapseField string
	cacheTTL      time.Duration
}

func NewQuery() *QueryBuilder {
	return &QueryBuilder{
		size: -1,
	}
}

func (q *QueryBuilder) CacheFor(ttl time.Duration) *QueryBuilder {
	q.cacheTTL = ttl

	return q
}

func (q *QueryBuilder) CacheTTL() time.Duration {
	return q.cacheTTL
}

func (q *QueryBuilder) Filter(conditions ...Condition) *QueryBuilder {
	for _, c := range conditions {
		if c != nil {
			q.filter = append(q.filter, c.Build())
		}
	}

	return q
}

func (q *QueryBuilder) Exclude(conditions ...Condition) *QueryBuilder {
	for _, c := range conditions {
		if c != nil {
			q.mustNot = append(q.mustNot, c.Build())
		}
	}

	return q
}

func (q *QueryBuilder) Should(conditions ...Condition) *QueryBuilder {
	for _, c := range conditions {
		if c != nil {
			q.should = append(q.should, c.Build())
		}
	}

	return q
}

func (q *QueryBuilder) PostFilter(conditions ...Condition) *QueryBuilder {
	for _, c := range conditions {
		if c != nil {
			q.postFilter = append(q.postFilter, c.Build())
		}
	}

	return q
}

func (q *QueryBuilder) MustAll(conditions ...Condition) *QueryBuilder {
	for _, c := range conditions {
		if c != nil {
			q.must = append(q.must, c.Build())
		}
	}

	return q
}

func (q *QueryBuilder) Must(clause map[string]any) *QueryBuilder {
	q.must = append(q.must, clause)

	return q
}

func (q *QueryBuilder) FilterTermsUint(field string, values []uint64) *QueryBuilder {
	if len(values) == 0 {
		return q
	}

	q.filter = append(q.filter, map[string]any{
		"terms": map[string]any{field: values},
	})

	return q
}

func (q *QueryBuilder) FilterTermsString(field string, values []string) *QueryBuilder {
	if len(values) == 0 {
		return q
	}

	q.filter = append(q.filter, map[string]any{
		"terms": map[string]any{field: values},
	})

	return q
}

func (q *QueryBuilder) FilterTerm(field string, value any) *QueryBuilder {
	q.filter = append(q.filter, map[string]any{
		"term": map[string]any{field: value},
	})

	return q
}

func (q *QueryBuilder) FilterRange(field string, gte, lte any) *QueryBuilder {
	rangeClause := map[string]any{}

	if gte != nil {
		rangeClause["gte"] = gte
	}

	if lte != nil {
		rangeClause["lte"] = lte
	}

	if len(rangeClause) == 0 {
		return q
	}

	q.filter = append(q.filter, map[string]any{
		"range": map[string]any{field: rangeClause},
	})

	return q
}

func (q *QueryBuilder) FilterRangeUint(field string, gte, lte uint64) *QueryBuilder {
	var gteVal, lteVal any

	if gte > 0 {
		gteVal = gte
	}

	if lte > 0 {
		lteVal = lte
	}

	return q.FilterRange(field, gteVal, lteVal)
}

func (q *QueryBuilder) FilterRangeInt(field string, gte, lte int) *QueryBuilder {
	var gteVal, lteVal any

	if gte > 0 {
		gteVal = gte
	}

	if lte > 0 {
		lteVal = lte
	}

	return q.FilterRange(field, gteVal, lteVal)
}

func (q *QueryBuilder) FilterNested(path string, build func(n *QueryBuilder)) *QueryBuilder {
	nested := NewQuery()
	build(nested)

	if len(nested.filter) == 0 && len(nested.must) == 0 && len(nested.should) == 0 {
		return q
	}

	nestedBool := nested.buildBoolQuery()

	q.filter = append(q.filter, map[string]any{
		"nested": map[string]any{
			"path":  path,
			"query": map[string]any{"bool": nestedBool},
		},
	})

	return q
}

func (q *QueryBuilder) FilterRaw(clause map[string]any) *QueryBuilder {
	q.filter = append(q.filter, clause)

	return q
}

func (q *QueryBuilder) MustNotTermsUint(field string, values []uint64) *QueryBuilder {
	if len(values) == 0 {
		return q
	}

	q.mustNot = append(q.mustNot, map[string]any{
		"terms": map[string]any{field: values},
	})

	return q
}

func (q *QueryBuilder) MustNotTermsString(field string, values []string) *QueryBuilder {
	if len(values) == 0 {
		return q
	}

	q.mustNot = append(q.mustNot, map[string]any{
		"terms": map[string]any{field: values},
	})

	return q
}

func (q *QueryBuilder) ShouldMatch(field, query string, boost float64) *QueryBuilder {
	matchClause := map[string]any{
		"query":    query,
		"operator": "and",
	}

	if boost != 0 {
		matchClause["boost"] = boost
	}

	q.should = append(q.should, map[string]any{
		"match": map[string]any{field: matchClause},
	})

	return q
}

func (q *QueryBuilder) ShouldMatchFuzzy(field, query string, boost float64, fuzziness string, prefixLen int) *QueryBuilder {
	matchClause := map[string]any{
		"query":         query,
		"operator":      "and",
		"fuzziness":     fuzziness,
		"prefix_length": prefixLen,
	}

	if boost != 0 {
		matchClause["boost"] = boost
	}

	q.should = append(q.should, map[string]any{
		"match": map[string]any{field: matchClause},
	})

	return q
}

func (q *QueryBuilder) ShouldPrefix(field, value string) *QueryBuilder {
	q.should = append(q.should, map[string]any{
		"prefix": map[string]any{
			field: map[string]any{"value": value},
		},
	})

	return q
}

func (q *QueryBuilder) MinimumShouldMatch(n int) *QueryBuilder {
	q.minShould = n

	return q
}

func (q *QueryBuilder) Size(n int) *QueryBuilder {
	q.size = n

	return q
}

func (q *QueryBuilder) From(n int) *QueryBuilder {
	q.from = n

	return q
}

func (q *QueryBuilder) Page(pager *Pager) *QueryBuilder {
	if pager != nil {
		q.size = pager.Limit
		q.from = pager.Offset
	}

	return q
}

func (q *QueryBuilder) Source(fields ...string) *QueryBuilder {
	q.source = fields

	return q
}

func (q *QueryBuilder) SortBy(field string, desc bool) *QueryBuilder {
	order := "asc"
	if desc {
		order = "desc"
	}

	q.sort = append(q.sort, map[string]any{
		field: map[string]any{"order": order},
	})

	return q
}

func (q *QueryBuilder) SortByField(field Field, desc bool) *QueryBuilder {
	return q.SortBy(field.FieldName(), desc)
}

func (q *QueryBuilder) SortByFieldMissing(field Field, desc bool, missing string) *QueryBuilder {
	order := "asc"
	if desc {
		order = "desc"
	}

	q.sort = append(q.sort, map[string]any{
		field.FieldName(): map[string]any{"order": order, "missing": missing},
	})

	return q
}

func (q *QueryBuilder) SortByFieldAllowMissing(field Field, desc bool, unmappedType string) *QueryBuilder {
	order := "asc"
	if desc {
		order = "desc"
	}

	q.sort = append(q.sort, map[string]any{
		field.FieldName(): map[string]any{"order": order, "unmapped_type": unmappedType},
	})

	return q
}

func (q *QueryBuilder) SortByScore(desc bool) *QueryBuilder {
	return q.SortBy("_score", desc)
}

func (q *QueryBuilder) WithSort(options, defaults []SortOption) *QueryBuilder {
	if len(options) == 0 {
		options = defaults
	}

	for _, opt := range options {
		q.SortBy(opt.Field, opt.Desc)
	}

	return q
}

func (q *QueryBuilder) PostFilterTermsUint(field string, values []uint64) *QueryBuilder {
	if len(values) == 0 {
		return q
	}

	q.postFilter = append(q.postFilter, map[string]any{
		"terms": map[string]any{field: values},
	})

	return q
}

func (q *QueryBuilder) TermsAgg(field Field, size int) *QueryBuilder {
	if q.aggs == nil {
		q.aggs = make(map[string]any)
	}

	q.aggs[AggName(field)] = map[string]any{
		"terms": map[string]any{
			"field": field.FieldName(),
			"size":  size,
		},
	}

	return q
}

func (q *QueryBuilder) RangeAgg(field Field, ranges []map[string]any) *QueryBuilder {
	if q.aggs == nil {
		q.aggs = make(map[string]any)
	}

	q.aggs[AggName(field)] = map[string]any{
		"range": map[string]any{
			"field":  field.FieldName(),
			"ranges": ranges,
		},
	}

	return q
}

func AggName(field Field) string {
	return field.FieldName() + "_agg"
}

func MinAggName(field Field) string {
	return "min_" + field.FieldName()
}

func MaxAggName(field Field) string {
	return "max_" + field.FieldName()
}

func (q *QueryBuilder) MinAgg(field Field) *QueryBuilder {
	if q.aggs == nil {
		q.aggs = make(map[string]any)
	}

	q.aggs[MinAggName(field)] = map[string]any{
		"min": map[string]any{"field": field.FieldName()},
	}

	return q
}

func (q *QueryBuilder) MaxAgg(field Field) *QueryBuilder {
	if q.aggs == nil {
		q.aggs = make(map[string]any)
	}

	q.aggs[MaxAggName(field)] = map[string]any{
		"max": map[string]any{"field": field.FieldName()},
	}

	return q
}

func (q *QueryBuilder) FilterAgg(name string, filter Condition) *QueryBuilder {
	if q.aggs == nil {
		q.aggs = make(map[string]any)
	}

	q.aggs[name] = map[string]any{
		"filter": filter.Build(),
	}

	return q
}

func (q *QueryBuilder) FilterTermsAgg(name string, filter Condition, subField Field, size int) *QueryBuilder {
	if q.aggs == nil {
		q.aggs = make(map[string]any)
	}

	q.aggs[name] = map[string]any{
		"filter": filter.Build(),
		"aggs": map[string]any{
			AggName(subField): map[string]any{
				"terms": map[string]any{
					"field": subField.FieldName(),
					"size":  size,
				},
			},
		},
	}

	return q
}

func (q *QueryBuilder) Agg(name string, agg map[string]any) *QueryBuilder {
	if q.aggs == nil {
		q.aggs = make(map[string]any)
	}

	q.aggs[name] = agg

	return q
}

func (q *QueryBuilder) CollapseBy(field Field) *QueryBuilder {
	q.collapseField = field.FieldName()

	return q
}

func (q *QueryBuilder) FunctionScore(functions []map[string]any, scoreMode, boostMode string) *QueryBuilder {
	q.fnScore = &fnScoreOpts{
		functions: functions,
		scoreMode: scoreMode,
		boostMode: boostMode,
	}

	return q
}

func (q *QueryBuilder) buildBoolQuery() map[string]any {
	boolQuery := make(map[string]any)

	if len(q.must) > 0 {
		boolQuery["must"] = q.must
	}

	if len(q.filter) > 0 {
		boolQuery["filter"] = q.filter
	}

	if len(q.mustNot) > 0 {
		boolQuery["must_not"] = q.mustNot
	}

	if len(q.should) > 0 {
		boolQuery["should"] = q.should
	}

	if q.minShould > 0 {
		boolQuery["minimum_should_match"] = q.minShould
	}

	return boolQuery
}

func (q *QueryBuilder) Build() map[string]any {
	boolQuery := q.buildBoolQuery()

	var queryClause map[string]any
	if len(boolQuery) == 0 {
		queryClause = map[string]any{
			"match_all": map[string]any{},
		}
	} else {
		queryClause = map[string]any{
			"bool": boolQuery,
		}
	}

	if q.fnScore != nil {
		queryClause = map[string]any{
			"function_score": map[string]any{
				"query":      queryClause,
				"functions":  q.fnScore.functions,
				"score_mode": q.fnScore.scoreMode,
				"boost_mode": q.fnScore.boostMode,
			},
		}
	}

	result := map[string]any{
		"query": queryClause,
	}

	if q.size >= 0 {
		result["size"] = q.size
	}

	if q.from > 0 {
		result["from"] = q.from
	}

	if len(q.source) > 0 {
		result["_source"] = q.source
	}

	if len(q.sort) > 0 {
		result["sort"] = q.sort
	}

	if len(q.postFilter) > 0 {
		result["post_filter"] = map[string]any{
			"bool": map[string]any{
				"filter": q.postFilter,
			},
		}
	}

	if len(q.aggs) > 0 {
		result["aggs"] = q.aggs
	}

	if q.collapseField != "" {
		result["collapse"] = map[string]any{"field": q.collapseField}
	}

	return result
}
