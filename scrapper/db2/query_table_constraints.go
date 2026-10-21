package db2

import (
	"context"
	_ "embed"

	dwhexecdb2 "github.com/getsynq/dwhsupport/exec/db2"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/samber/lo"
)

//go:embed query_table_constraints.sql
var queryTableConstraintsSql string

type db2ConstraintRow struct {
	Schema               string  `db:"schema"`
	Table                string  `db:"table"`
	ConstraintName       string  `db:"constraint_name"`
	ColumnName           *string `db:"column_name"`
	ConstraintType       string  `db:"constraint_type"`
	ColumnPosition       int32   `db:"column_position"`
	ConstraintExpression *string `db:"constraint_expression"`
	IsEnforced           int     `db:"is_enforced"`
	ReferencedSchema     *string `db:"referenced_schema"`
	ReferencedTable      *string `db:"referenced_table"`
	ReferencedColumnName *string `db:"referenced_column_name"`
}

func (e *Db2Scrapper) QueryTableConstraints(ctx context.Context) ([]*scrapper.TableConstraintRow, error) {
	database, err := e.currentServer(ctx)
	if err != nil {
		return nil, err
	}
	sql := scope.AppendScopeConditions(ctx, queryTableConstraintsSql, "", `"schema"`, `"table"`)
	rows, err := dwhexecdb2.NewQuerier[db2ConstraintRow](e.executor).QueryMany(ctx, sql)
	if err != nil {
		return nil, err
	}

	results := make([]*scrapper.TableConstraintRow, 0, len(rows))
	for _, row := range rows {
		enforced := row.IsEnforced == 1
		r := &scrapper.TableConstraintRow{
			Instance:       e.conf.Hostname,
			Database:       database,
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
		if row.ReferencedTable != nil {
			// A foreign key cannot reference another database.
			r.ReferencedDatabase = database
			r.ReferencedSchema = lo.FromPtr(row.ReferencedSchema)
			r.ReferencedTable = *row.ReferencedTable
			r.ReferencedColumnName = lo.FromPtr(row.ReferencedColumnName)
		}
		results = append(results, r)
	}
	return results, nil
}

func (e *Db2Scrapper) currentServer(ctx context.Context) (string, error) {
	var name string
	rows, err := e.executor.QueryRows(ctx, "SELECT CURRENT SERVER FROM SYSIBM.SYSDUMMY1")
	if err != nil {
		return "", err
	}
	defer rows.Close()
	if rows.Next() {
		if err := rows.Scan(&name); err != nil {
			return "", err
		}
	}
	return name, rows.Err()
}
