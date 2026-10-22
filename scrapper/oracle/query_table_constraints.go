package oracle

import (
	"context"
	_ "embed"

	dwhexecoracle "github.com/getsynq/dwhsupport/exec/oracle"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/samber/lo"
)

//go:embed query_table_constraints.sql
var queryTableConstraintsSql string

//go:embed query_function_based_indexes.sql
var queryFunctionBasedIndexesSql string

type oracleConstraintRow struct {
	Schema               string  `db:"schema"`
	Table                string  `db:"table"`
	ConstraintName       string  `db:"constraint_name"`
	ColumnName           *string `db:"column_name"`
	ConstraintType       string  `db:"constraint_type"`
	ColumnPosition       int32   `db:"column_position"`
	ConstraintExpression *string `db:"constraint_expression"`
	IsEnforced           int     `db:"is_enforced"`
	IndexType            *string `db:"index_type"`
	ReferencedSchema     *string `db:"referenced_schema"`
	ReferencedTable      *string `db:"referenced_table"`
	ReferencedColumnName *string `db:"referenced_column_name"`
}

type oracleFunctionBasedIndexRow struct {
	Schema           string  `db:"schema"`
	Table            string  `db:"table"`
	ConstraintName   string  `db:"constraint_name"`
	ColumnName       string  `db:"column_name"`
	ColumnExpression *string `db:"column_expression"`
	ConstraintType   string  `db:"constraint_type"`
	ColumnPosition   int32   `db:"column_position"`
	IsEnforced       int     `db:"is_enforced"`
	IndexType        string  `db:"index_type"`
}

func (e *OracleScrapper) QueryTableConstraints(ctx context.Context) ([]*scrapper.TableConstraintRow, error) {
	sql := scope.AppendScopeConditions(ctx, queryTableConstraintsSql, "", "\"schema\"", "\"table\"")
	rows, err := dwhexecoracle.NewQuerier[oracleConstraintRow](e.executor).QueryMany(ctx, sql)
	if err != nil {
		return nil, err
	}

	var results []*scrapper.TableConstraintRow
	for _, row := range rows {
		enforced := row.IsEnforced == 1
		r := &scrapper.TableConstraintRow{
			Instance:       e.conf.Host,
			Database:       e.conf.ServiceName,
			Schema:         row.Schema,
			Table:          row.Table,
			ConstraintName: row.ConstraintName,
			ConstraintType: row.ConstraintType,
			ColumnPosition: row.ColumnPosition,
			IsEnforced:     &enforced,
			IndexType:      lo.FromPtr(row.IndexType),
		}
		if row.ColumnName != nil {
			r.ColumnName = *row.ColumnName
		}
		if row.ConstraintExpression != nil {
			r.ConstraintExpression = *row.ConstraintExpression
		}
		if row.ReferencedTable != nil {
			// A foreign key cannot reference another database.
			r.ReferencedDatabase = e.conf.ServiceName
			r.ReferencedSchema = lo.FromPtr(row.ReferencedSchema)
			r.ReferencedTable = *row.ReferencedTable
			r.ReferencedColumnName = lo.FromPtr(row.ReferencedColumnName)
		}
		results = append(results, r)
	}

	functionBased, err := e.queryFunctionBasedIndexes(ctx)
	if err != nil {
		return nil, err
	}
	return append(results, functionBased...), nil
}

// queryFunctionBasedIndexes reads the function-based indexes, one row per index
// column, each named by its expression where it is one.
func (e *OracleScrapper) queryFunctionBasedIndexes(ctx context.Context) ([]*scrapper.TableConstraintRow, error) {
	sql := scope.AppendScopeConditions(ctx, queryFunctionBasedIndexesSql, "", "i.table_owner", "i.table_name")
	rows, err := dwhexecoracle.NewQuerier[oracleFunctionBasedIndexRow](e.executor).QueryMany(ctx, sql)
	if err != nil {
		return nil, err
	}
	return lo.Map(rows, func(row *oracleFunctionBasedIndexRow, _ int) *scrapper.TableConstraintRow {
		return functionBasedIndexRow(e.conf.Host, e.conf.ServiceName, row)
	}), nil
}

// functionBasedIndexRow is one column of a function-based index. A column that
// is an expression is named by the expression, as Oracle keeps it, in place of
// the hidden SYS_NC column the index lists.
func functionBasedIndexRow(host, serviceName string, row *oracleFunctionBasedIndexRow) *scrapper.TableConstraintRow {
	return &scrapper.TableConstraintRow{
		Instance:       host,
		Database:       serviceName,
		Schema:         row.Schema,
		Table:          row.Table,
		ConstraintName: row.ConstraintName,
		ColumnName:     lo.CoalesceOrEmpty(lo.FromPtr(row.ColumnExpression), row.ColumnName),
		ConstraintType: row.ConstraintType,
		ColumnPosition: row.ColumnPosition,
		IsEnforced:     lo.ToPtr(row.IsEnforced == 1),
		IndexType:      row.IndexType,
	}
}
