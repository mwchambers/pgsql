package pgsql_test

import (
	"testing"

	"github.com/mwchambers/pgsql"
	"github.com/stretchr/testify/assert"
)

func TestSelect(t *testing.T) {
	a := pgsql.Select("a, b, c")
	sql, args := pgsql.Build(a)
	assert.Equal(t, "select a, b, c", sql)
	assert.Empty(t, args)
}

func TestFrom(t *testing.T) {
	a := pgsql.From("people")
	sql, args := pgsql.Build(a)
	assert.Equal(t, "select * from people", sql)
	assert.Empty(t, args)
}

func TestFromJoin(t *testing.T) {
	a := pgsql.Select("u.id, g.name")
	from := "user as u join group as g on u.group_id = g.id"
	a.From(from)
	want := "select u.id, g.name from " + from
	sql, args := pgsql.Build(a)
	assert.Equal(t, want, sql)
	assert.Empty(t, args)
}

func TestFromJoinSelectAppends(t *testing.T) {
	a := pgsql.Select("u.id, g.name")
	from := "user as u join group as g on u.group_id = g.id"
	a.From(from)
	a.Select("u.forename")
	want := "select u.id, g.name, u.forename from " + from
	sql, args := pgsql.Build(a)
	assert.Equal(t, want, sql)
	assert.Empty(t, args)
}

func TestWhere(t *testing.T) {
	a := pgsql.Where("id=?", 2).From("people")
	sql, args := pgsql.Build(a)
	assert.Equal(t, "select * from people where (id=$1)", sql)
	assert.Equal(t, []interface{}{2}, args)
}

func TestWhereBrackets(t *testing.T) {
	a := pgsql.Where("id=?", 2).From("people")
	a.Where("b=? OR c=?", 3, 4)
	sql, args := pgsql.Build(a)
	assert.Equal(t, "select * from people where (id=$1) and (b=$2 OR c=$3)", sql)
	assert.Equal(t, []interface{}{2, 3, 4}, args)
}

func TestAnd(t *testing.T) {
	a := pgsql.Select("id").From("people").Where("a>?", 2)
	a.And("b=? OR c=?", false, true)
	sql, args := pgsql.Build(a)
	assert.Equal(t, "select id from people where (a>$1) and (b=$2 OR c=$3)", sql)
	assert.Equal(t, []interface{}{2, false, true}, args)
}

func TestSelectStatementDistinct(t *testing.T) {
	a := pgsql.Select("a, b, c").Distinct(true)
	sql, args := pgsql.Build(a)
	assert.Equal(t, "select distinct a, b, c", sql)
	assert.Empty(t, args)
}

func TestSelectStatementDistinctOn(t *testing.T) {
	a := pgsql.Select("a, b, c").DistinctOn("a")
	sql, args := pgsql.Build(a)
	assert.Equal(t, "select distinct on (a) a, b, c", sql)
	assert.Empty(t, args)

	a.DistinctOn("b")
	sql, args = pgsql.Build(a)
	assert.Equal(t, "select distinct on (a, b) a, b, c", sql)
	assert.Empty(t, args)
}

func TestSelectStatementMultipleSelect(t *testing.T) {
	a := pgsql.Select("a").Select("b")
	sql, args := pgsql.Build(a)
	assert.Equal(t, "select a, b", sql)
	assert.Empty(t, args)
}

func TestSelectStatementReplaceSelect(t *testing.T) {
	a := pgsql.Select("a").ReplaceSelect("b")
	sql, args := pgsql.Build(a)
	assert.Equal(t, "select b", sql)
	assert.Empty(t, args)
}

func TestSelectStatementWhere(t *testing.T) {
	a := pgsql.Select("a, b, c").From("t").Where("foo=?", 42)
	sql, args := pgsql.Build(a)
	assert.Equal(t, "select a, b, c from t where (foo=$1)", sql)
	assert.Equal(t, []interface{}{42}, args)
}

func TestSelectStatementOrder(t *testing.T) {
	a := pgsql.Select("a, b, c").From("t").Order("c desc")
	sql, args := pgsql.Build(a)
	assert.Equal(t, "select a, b, c from t order by c desc", sql)
	assert.Empty(t, args)

	a.Order("a asc")
	sql, args = pgsql.Build(a)
	assert.Equal(t, "select a, b, c from t order by c desc, a asc", sql)
	assert.Empty(t, args)
}

func TestSelectStatementReplaceOrder(t *testing.T) {
	a := pgsql.Select("a, b, c").From("t").Order("c desc")
	sql, args := pgsql.Build(a)
	assert.Equal(t, "select a, b, c from t order by c desc", sql)
	assert.Empty(t, args)

	a.ReplaceOrder("a asc")
	sql, args = pgsql.Build(a)
	assert.Equal(t, "select a, b, c from t order by a asc", sql)
	assert.Empty(t, args)
}

func TestSelectStatementLimitAndOffset(t *testing.T) {
	a := pgsql.Select("a, b, c").From("t").Order("c desc")
	a.Limit(5)
	sql, args := pgsql.Build(a)
	assert.Equal(t, "select a, b, c from t order by c desc limit 5", sql)
	assert.Empty(t, args)

	a.Offset(10)
	sql, args = pgsql.Build(a)
	assert.Equal(t, "select a, b, c from t order by c desc limit 5 offset 10", sql)
	assert.Empty(t, args)
}

func TestSelectStatementGroupBy(t *testing.T) {
	a := pgsql.Select("a").From("t").Order("a desc").GroupBy("a")
	sql, args := pgsql.Build(a)
	assert.Equal(t, "select a from t group by a order by a desc", sql)
	assert.Empty(t, args)

	a.GroupBy("b")
	sql, args = pgsql.Build(a)
	assert.Equal(t, "select a from t group by a, b order by a desc", sql)
	assert.Empty(t, args)
}

func TestSelectStatementApply(t *testing.T) {
	a := pgsql.Select("a, b, c").From("t").Order("c desc")
	sql, args := pgsql.Build(a)
	assert.Equal(t, "select a, b, c from t order by c desc", sql)
	assert.Empty(t, args)

	b := pgsql.Where("d=?", 42).Limit(5)
	a.Apply(b)

	sql, args = pgsql.Build(a)
	assert.Equal(t, "select a, b, c from t where (d=$1) order by c desc limit 5", sql)
	assert.Equal(t, []interface{}{42}, args)
}

func TestSelectStatementApplyReplaceSelect(t *testing.T) {
	a := pgsql.Select("a, b, c").From("t").Order("c desc")
	sql, args := pgsql.Build(a)
	assert.Equal(t, "select a, b, c from t order by c desc", sql)
	assert.Empty(t, args)

	b := pgsql.ReplaceSelect("a")
	a.Apply(b)

	sql, args = pgsql.Build(a)
	assert.Equal(t, "select a from t order by c desc", sql)
	assert.Empty(t, args)

	c := pgsql.Select("d")
	a.Apply(c)

	sql, args = pgsql.Build(a)
	assert.Equal(t, "select a, d from t order by c desc", sql)
	assert.Empty(t, args)
}

func TestSelectStatementApplyReplaceOrder(t *testing.T) {
	a := pgsql.Select("a, b, c").From("t").Order("c desc")
	sql, args := pgsql.Build(a)
	assert.Equal(t, "select a, b, c from t order by c desc", sql)
	assert.Empty(t, args)

	b := pgsql.ReplaceOrder("a desc")
	a.Apply(b)

	sql, args = pgsql.Build(a)
	assert.Equal(t, "select a, b, c from t order by a desc", sql)
	assert.Empty(t, args)

	c := pgsql.Order("d")
	a.Apply(c)

	sql, args = pgsql.Build(a)
	assert.Equal(t, "select a, b, c from t order by a desc, d", sql)
	assert.Empty(t, args)
}

func TestSelectStatementBuildChain(t *testing.T) {
	sql, args := pgsql.Select("a, b, c").From("t").Where("a > ?", 42).Build()

	assert.Equal(t, "select a, b, c from t where (a > $1)", sql)

	assert.Equal(t, []interface{}{42}, args)
}
