package orm

import (
	"strconv"
	"strings"
)

type sortClause struct {
	column string
	asc    bool
}

// DBQuery is a type-safe query builder for MySQL queries.
type DBQuery struct {
	pager       *Pager
	sortClauses []sortClause
	conditions  []Condition
	rawWhere    Where
}

// NewQuery creates a new empty DBQuery.
func NewQuery() *DBQuery {
	return &DBQuery{}
}

// Pager sets the pagination for the query.
func (q *DBQuery) Pager(p *Pager) *DBQuery {
	q.pager = p
	return q
}

// SortByASC adds an ascending sort clause.
func (q *DBQuery) SortByASC(f Field) *DBQuery {
	q.sortClauses = append(q.sortClauses, sortClause{column: f.ColumnName(), asc: true})
	return q
}

// SortByDESC adds a descending sort clause.
func (q *DBQuery) SortByDESC(f Field) *DBQuery {
	q.sortClauses = append(q.sortClauses, sortClause{column: f.ColumnName(), asc: false})
	return q
}

// Filter adds typed conditions to the query.
func (q *DBQuery) Filter(conditions ...Condition) *DBQuery {
	q.conditions = append(q.conditions, conditions...)
	return q
}

// FilterWhere adds a raw Where clause to the query.
func (q *DBQuery) FilterWhere(w Where) *DBQuery {
	q.rawWhere = w
	return q
}

// GetConditions returns typed conditions for generated code's unique index detection.
func (q *DBQuery) GetConditions() []Condition {
	return q.conditions
}

// BuildWhereClause builds the WHERE portion (without "WHERE" keyword).
// Returns empty string and nil params if no conditions.
func (q *DBQuery) BuildWhereClause() (string, []any) {
	var b strings.Builder
	params := q.writePredicates(&b, q.rawWhereSQL())
	return b.String(), params
}

// BuildOrderClause returns "ORDER BY `col1` ASC, `col2` DESC" or empty string.
func (q *DBQuery) BuildOrderClause() string {
	var b strings.Builder
	q.writeOrder(&b, "ORDER BY ")
	return b.String()
}

// BuildLimitClause returns "LIMIT offset,count" or empty string.
func (q *DBQuery) BuildLimitClause() string {
	var b strings.Builder
	q.writeLimit(&b, "LIMIT ")
	return b.String()
}

// WriteSelect appends the query to b after prefix: the WHERE clause built from
// the typed conditions and the raw Where, then the ORDER BY clause when
// withOrder is set and the pager's LIMIT clause when withLimit is set.
// excludeFakeDeleted adds the `FakeDelete` = 0 guard unless the raw Where
// allows fake-deleted rows. It returns the bound parameters in SQL order.
// The builder grows once up front, so a typical query costs one string and
// one parameter slice.
func (q *DBQuery) WriteSelect(b *strings.Builder, prefix string, excludeFakeDeleted, withOrder, withLimit bool) []any {
	b.Grow(len(prefix) + q.sizeHint())
	b.WriteString(prefix)
	params := q.writeWhere(b, excludeFakeDeleted)
	if withOrder {
		q.writeOrder(b, " ORDER BY ")
	}
	if withLimit {
		q.writeLimit(b, " LIMIT ")
	}
	return params
}

func (q *DBQuery) rawWhereSQL() string {
	if q.rawWhere == nil {
		return ""
	}
	return q.rawWhere.String()
}

// sizeHint over-estimates the SQL the query appends, so the builder grows once.
func (q *DBQuery) sizeHint() int {
	n := 32 + 32*len(q.conditions) + 32*len(q.sortClauses)
	if q.rawWhere != nil {
		n += len(q.rawWhere.String()) + 5
	}
	for _, c := range q.conditions {
		if in, ok := c.(inCondition); ok {
			n += 2 * len(in.values)
		}
	}
	return n
}

// writeWhere appends " WHERE ..." including the optional fake-delete guard,
// or nothing when the query has no predicates.
func (q *DBQuery) writeWhere(b *strings.Builder, excludeFakeDeleted bool) []any {
	raw := q.rawWhereSQL()
	guard := excludeFakeDeleted && !q.IsWithFakeDeletes()
	hasPredicates := len(q.conditions) > 0 || raw != ""
	if !hasPredicates && !guard {
		return nil
	}
	b.WriteString(" WHERE ")
	params := q.writePredicates(b, raw)
	if guard {
		if hasPredicates {
			b.WriteString(" AND ")
		}
		b.WriteString("`FakeDelete` = 0")
	}
	return params
}

// writePredicates appends the typed conditions and the raw Where joined by AND.
func (q *DBQuery) writePredicates(b *strings.Builder, raw string) []any {
	var params []any
	if n := q.paramCount(raw != ""); n > 0 {
		params = make([]any, 0, n)
	}
	for i, c := range q.conditions {
		if i > 0 {
			b.WriteString(" AND ")
		}
		params = writeCondition(b, params, c)
	}
	if raw != "" {
		if len(q.conditions) > 0 {
			b.WriteString(" AND ")
		}
		b.WriteString(raw)
		params = append(params, q.rawWhere.GetParameters()...)
	}
	return params
}

func (q *DBQuery) paramCount(withRaw bool) int {
	n := 0
	for _, c := range q.conditions {
		n += conditionParamCount(c)
	}
	if withRaw {
		n += len(q.rawWhere.GetParameters())
	}
	return n
}

func (q *DBQuery) writeOrder(b *strings.Builder, prefix string) {
	for i, s := range q.sortClauses {
		if i == 0 {
			b.WriteString(prefix)
		} else {
			b.WriteString(", ")
		}
		writeColumn(b, s.column)
		if s.asc {
			b.WriteString(" ASC")
		} else {
			b.WriteString(" DESC")
		}
	}
}

func (q *DBQuery) writeLimit(b *strings.Builder, prefix string) {
	if q.pager == nil {
		return
	}
	var digits [20]byte
	b.WriteString(prefix)
	b.Write(strconv.AppendInt(digits[:0], int64((q.pager.CurrentPage-1)*q.pager.PageSize), 10))
	b.WriteByte(',')
	b.Write(strconv.AppendInt(digits[:0], int64(q.pager.PageSize), 10))
}

// PageSize returns the pager's page size, or 0 when the query has no pager.
func (q *DBQuery) PageSize() int {
	if q.pager == nil {
		return 0
	}
	return q.pager.PageSize
}

// IsWithFakeDeletes returns true if rawWhere has WithFakeDeletes set.
func (q *DBQuery) IsWithFakeDeletes() bool {
	if q.rawWhere != nil {
		return q.rawWhere.IsWithFakeDeletes()
	}
	return false
}
