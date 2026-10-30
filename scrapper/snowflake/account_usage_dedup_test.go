package snowflake

import (
	"fmt"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
)

func TestNewestByName(t *testing.T) {
	type row struct {
		name    string
		id      int64
		created *time.Time
	}
	nameOf := func(r row) objectName { return objectName{"DB", "S", r.name} }
	idOf := func(_ int, r row) int64 { return r.id }
	createdOf := func(r row) *time.Time { return r.created }
	ids := func(rows []row) []int64 { return lo.Map(rows, func(r row, _ int) int64 { return r.id }) }

	t.Run("keeps every row of the newest object, in the order they came", func(t *testing.T) {
		got := newestByName([]row{
			{"A", 2, &liveAt}, {"A", 1, &replacedAt}, {"B", 3, &replacedAt}, {"A", 2, &liveAt}, {"A", 1, &replacedAt},
		}, nameOf, idOf, createdOf)
		assert.Equal(t, []int64{2, 3, 2}, ids(got))
	})
	t.Run("a tie keeps the object listed first", func(t *testing.T) {
		got := newestByName([]row{{"A", 1, &liveAt}, {"A", 2, &liveAt}}, nameOf, idOf, createdOf)
		assert.Equal(t, []int64{1}, ids(got))
	})
	t.Run("an object without a creation time loses to one with it", func(t *testing.T) {
		got := newestByName([]row{{"A", 1, nil}, {"A", 2, &replacedAt}}, nameOf, idOf, createdOf)
		assert.Equal(t, []int64{2}, ids(got))
	})
	t.Run("names differing only in case are different objects", func(t *testing.T) {
		got := newestByName([]row{{"A", 1, &replacedAt}, {"a", 2, &liveAt}}, nameOf, idOf, createdOf)
		assert.Equal(t, []int64{1, 2}, ids(got))
	})
	t.Run("nothing in, nothing out", func(t *testing.T) {
		assert.Empty(t, newestByName([]row{}, nameOf, idOf, createdOf))
	})
}

func TestFirstPositionOfEachColumn(t *testing.T) {
	column := func(tableId int64, name string, position int32) *accountUsageColumnRow {
		return &accountUsageColumnRow{TableId: tableId, CatalogColumnRow: scrapper.CatalogColumnRow{Column: name, Position: position}}
	}
	got := firstPositionOfEachColumn([]*accountUsageColumnRow{
		column(1, "B", 3), column(1, "A", 1), column(1, "B", 2), column(2, "B", 5), column(1, "A", 1),
	})
	assert.Equal(t,
		[]string{"1.A.1", "1.B.2", "2.B.5"},
		lo.Map(got, func(row *scrapper.CatalogColumnRow, i int) string {
			return []string{"1", "1", "2"}[i] + "." + row.Column + "." + fmt.Sprint(row.Position)
		}),
	)
}
