package querybuilder

import (
	. "github.com/getsynq/dwhsupport/sqldialect"
	"github.com/samber/lo"
)

// MetaQueryBuilder provides dialect specific ways to query table metadata
//
// It also contains utility functions `Merge` and `Split`,
// which can be used to control the size and amount of requests sent.
type MetaQueryBuilder struct {
	tables []TableExpr
}

func NewMetaQueryBuilder(tables []TableExpr) *MetaQueryBuilder {
	return &MetaQueryBuilder{
		tables: tables,
	}
}

// Merge adds all tables from the other MetaQueryBuilder.
func (m *MetaQueryBuilder) Merge(other *MetaQueryBuilder) {
	m.tables = append(m.tables, other.tables...)
}

// Split the MetaQueryBuilder into multiple builders
// with a max tables count of `size`.
func (m *MetaQueryBuilder) Split(size int) []*MetaQueryBuilder {
	chunks := lo.Chunk(m.tables, size)
	return lo.Map(chunks, func(chunk []TableExpr, _ int) *MetaQueryBuilder {
		return &MetaQueryBuilder{
			tables: chunk,
		}
	})
}

func (m *MetaQueryBuilder) ToSql(dialect Dialect) (string, error) {
	return "", nil
}
