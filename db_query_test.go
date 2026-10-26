package orm

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDBQueryEmpty(t *testing.T) {
	q := NewQuery()

	clause, params := q.BuildWhereClause()
	assert.Equal(t, "", clause)
	assert.Nil(t, params)

	assert.Equal(t, "", q.BuildOrderClause())
	assert.Equal(t, "", q.BuildLimitClause())
	assert.Nil(t, q.GetConditions())
	assert.False(t, q.IsWithFakeDeletes())
}

func TestDBQueryFilterConditions(t *testing.T) {
	age := UintField{Column: "Age"}
	name := StringField{Column: "Name"}

	q := NewQuery().Filter(age.Gte(18), name.Is("John"))

	clause, params := q.BuildWhereClause()
	assert.Equal(t, "`Age` >= ? AND `Name` = ?", clause)
	assert.Equal(t, []any{uint64(18), "John"}, params)
}

func TestDBQueryFilterMultipleCalls(t *testing.T) {
	age := UintField{Column: "Age"}
	active := BoolField{Column: "Active"}

	q := NewQuery().
		Filter(age.Gte(18)).
		Filter(active.Is(true))

	clause, params := q.BuildWhereClause()
	assert.Equal(t, "`Age` >= ? AND `Active` = ?", clause)
	assert.Equal(t, []any{uint64(18), true}, params)
}

func TestDBQuerySortClauses(t *testing.T) {
	age := UintField{Column: "Age"}
	name := StringField{Column: "Name"}

	q := NewQuery().SortByASC(age).SortByDESC(name)
	assert.Equal(t, "ORDER BY `Age` ASC, `Name` DESC", q.BuildOrderClause())
}

func TestDBQuerySingleSort(t *testing.T) {
	f := UintField{Column: "ID"}
	q := NewQuery().SortByDESC(f)
	assert.Equal(t, "ORDER BY `ID` DESC", q.BuildOrderClause())
}

func TestDBQueryPager(t *testing.T) {
	p := NewPager(2, 10)
	q := NewQuery().Pager(p)
	assert.Equal(t, "LIMIT 10,10", q.BuildLimitClause())
}

func TestDBQueryFilterWhere(t *testing.T) {
	w := NewWhere("`Status` = ? AND `Level` > ?", "active", 5)
	q := NewQuery().FilterWhere(w)

	clause, params := q.BuildWhereClause()
	assert.Equal(t, "`Status` = ? AND `Level` > ?", clause)
	assert.Equal(t, []any{"active", 5}, params)
}

func TestDBQueryFilterWhereWithTypedConditions(t *testing.T) {
	age := UintField{Column: "Age"}
	w := NewWhere("`Status` = ?", "active")

	q := NewQuery().
		Filter(age.Gte(18)).
		FilterWhere(w)

	clause, params := q.BuildWhereClause()
	assert.Equal(t, "`Age` >= ? AND `Status` = ?", clause)
	assert.Equal(t, []any{uint64(18), "active"}, params)
}

func TestDBQueryGetConditions(t *testing.T) {
	age := UintField{Column: "Age"}
	name := StringField{Column: "Name"}

	q := NewQuery().Filter(age.Eq(25), name.Is("Alice"))

	conditions := q.GetConditions()
	assert.Len(t, conditions, 2)

	eq, ok := conditions[0].(EqCondition)
	assert.True(t, ok)
	assert.Equal(t, uint64(25), eq.EqValue())
}

func TestDBQueryIsWithFakeDeletes(t *testing.T) {
	q := NewQuery()
	assert.False(t, q.IsWithFakeDeletes())

	w := NewWhere("`Status` = ?", "active")
	q = NewQuery().FilterWhere(w)
	assert.False(t, q.IsWithFakeDeletes())

	w2 := NewWhere("`Status` = ?", "active").WithFakeDeletes()
	q = NewQuery().FilterWhere(w2)
	assert.True(t, q.IsWithFakeDeletes())
}

func TestDBQueryChaining(t *testing.T) {
	age := UintField{Column: "Age"}
	name := StringField{Column: "Name"}
	p := NewPager(1, 20)

	q := NewQuery().
		Filter(age.Gte(18), name.Is("John")).
		SortByASC(age).
		SortByDESC(name).
		Pager(p)

	clause, params := q.BuildWhereClause()
	assert.Equal(t, "`Age` >= ? AND `Name` = ?", clause)
	assert.Equal(t, []any{uint64(18), "John"}, params)
	assert.Equal(t, "ORDER BY `Age` ASC, `Name` DESC", q.BuildOrderClause())
	assert.Equal(t, "LIMIT 0,20", q.BuildLimitClause())
}

func TestDBQueryFilterWhereEmptyString(t *testing.T) {
	w := NewWhere("")
	q := NewQuery().FilterWhere(w)

	clause, params := q.BuildWhereClause()
	assert.Equal(t, "", clause)
	assert.Nil(t, params)
}

func TestDBQueryWriteSelectEmpty(t *testing.T) {
	var b strings.Builder
	params := NewQuery().WriteSelect(&b, "SELECT `ID` FROM `t`", false, true, true)
	assert.Equal(t, "SELECT `ID` FROM `t`", b.String())
	assert.Nil(t, params)
}

func TestDBQueryWriteSelectFullQuery(t *testing.T) {
	age := UintField{Column: "Age"}
	name := StringField{Column: "Name"}
	q := NewQuery().Filter(age.Gte(18), name.In("a", "b"), NullableUintField{Column: "Age"}.IsNull().Not()).
		SortByASC(age).SortByDESC(name).Pager(NewPager(3, 10))

	var b strings.Builder
	params := q.WriteSelect(&b, "SELECT `ID` FROM `t`", false, true, true)
	assert.Equal(t, "SELECT `ID` FROM `t` WHERE `Age` >= ? AND `Name` IN (?,?) AND NOT (`Age` IS NULL)"+
		" ORDER BY `Age` ASC, `Name` DESC LIMIT 20,10", b.String())
	assert.Equal(t, []any{uint64(18), "a", "b"}, params)
	assert.Equal(t, 3, cap(params), "parameters are sized up front")

	b.Reset()
	params = q.WriteSelect(&b, "SELECT COUNT(*) FROM `t`", false, false, false)
	assert.Equal(t, "SELECT COUNT(*) FROM `t` WHERE `Age` >= ? AND `Name` IN (?,?) AND NOT (`Age` IS NULL)", b.String())
	assert.Equal(t, []any{uint64(18), "a", "b"}, params)
}

func TestDBQueryWriteSelectFakeDeleteGuard(t *testing.T) {
	age := UintField{Column: "Age"}

	var b strings.Builder
	params := NewQuery().WriteSelect(&b, "SELECT `ID` FROM `t`", true, true, true)
	assert.Equal(t, "SELECT `ID` FROM `t` WHERE `FakeDelete` = 0", b.String())
	assert.Nil(t, params)

	b.Reset()
	params = NewQuery().Filter(age.Gte(18)).WriteSelect(&b, "SELECT `ID` FROM `t`", true, true, true)
	assert.Equal(t, "SELECT `ID` FROM `t` WHERE `Age` >= ? AND `FakeDelete` = 0", b.String())
	assert.Equal(t, []any{uint64(18)}, params)

	b.Reset()
	raw := NewWhere("`Name` = ?", "x").WithFakeDeletes()
	params = NewQuery().Filter(age.Gte(18)).FilterWhere(raw).WriteSelect(&b, "SELECT `ID` FROM `t`", true, true, true)
	assert.Equal(t, "SELECT `ID` FROM `t` WHERE `Age` >= ? AND `Name` = ?", b.String())
	assert.Equal(t, []any{uint64(18), "x"}, params)
}

type customTestCondition struct{}

func (customTestCondition) ToSQL() (string, []any) { return "`Custom` BETWEEN ? AND ?", []any{1, 2} }
func (customTestCondition) columnName() string     { return "Custom" }
func (c customTestCondition) Not() Condition       { return notCondition{inner: c} }

func TestDBQueryWriteSelectCustomCondition(t *testing.T) {
	var b strings.Builder
	q := NewQuery().Filter(customTestCondition{}, customTestCondition{}.Not())
	params := q.WriteSelect(&b, "SELECT `ID` FROM `t`", false, false, false)
	assert.Equal(t, "SELECT `ID` FROM `t` WHERE `Custom` BETWEEN ? AND ? AND NOT (`Custom` BETWEEN ? AND ?)", b.String())
	assert.Equal(t, []any{1, 2, 1, 2}, params)

	clause, params := q.BuildWhereClause()
	assert.Equal(t, "`Custom` BETWEEN ? AND ? AND NOT (`Custom` BETWEEN ? AND ?)", clause)
	assert.Equal(t, []any{1, 2, 1, 2}, params)
}

func TestDBQueryWriteSelectAllocations(t *testing.T) {
	age := UintField{Column: "Age"}
	q := NewQuery().Filter(age.Gte(18)).SortByASC(age).Pager(NewPager(1, 10))
	allocs := testing.AllocsPerRun(100, func() {
		var b strings.Builder
		params := q.WriteSelect(&b, "SELECT `ID` FROM `t`", true, true, true)
		if len(params) != 1 || b.Len() == 0 {
			t.Fatal("unexpected query")
		}
	})
	assert.LessOrEqual(t, allocs, 2.0, "one SQL string and one parameter slice")
}
