package oracle

import (
	"context"
	_ "embed"

	dwhexecoracle "github.com/getsynq/dwhsupport/exec/oracle"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_table_constraints.sql
var queryTableConstraintsSql string

type oracleConstraintRow struct {
	Schema               string  `db:"schema"`
	Table                string  `db:"table"`
	ConstraintName       string  `db:"constraint_name"`
	ColumnName           *string `db:"column_name"`
	ConstraintType       string  `db:"constraint_type"`
	ColumnPosition       int32   `db:"column_position"`
	ConstraintExpression *string `db:"constraint_expression"`
	IsEnforced           int     `db:"is_enforced"`
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
		}
		if row.ColumnName != nil {
			r.ColumnName = *row.ColumnName
		}
		if row.ConstraintExpression != nil {
			r.ConstraintExpression = *row.ConstraintExpression
		}
		results = append(results, r)
	}

	return results, nil
}
