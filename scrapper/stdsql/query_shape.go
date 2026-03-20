package stdsql

import (
	"context"
	"fmt"

	"github.com/getsynq/dwhsupport/exec/querystats"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/jmoiron/sqlx"
)

// RowQuerier is the minimal interface for executing queries that return rows.
// It is satisfied by all StdSqlExecutor implementations (which add SQL comment
// enrichment, query tagging, and stats collection) and by RawDB for tests.
type RowQuerier interface {
	QueryRows(ctx context.Context, q string, args ...interface{}) (*sqlx.Rows, error)
}

// RawDB wraps a *sqlx.DB to satisfy RowQuerier without any enrichment.
// Use this only in tests; production code should pass an executor.
type RawDB struct{ DB *sqlx.DB }

func (r *RawDB) QueryRows(ctx context.Context, q string, args ...interface{}) (*sqlx.Rows, error) {
	return r.DB.QueryxContext(ctx, q, args...)
}

func QueryShape(ctx context.Context, db RowQuerier, sql string) ([]*scrapper.QueryShapeColumn, error) {
	wrappedSQL := fmt.Sprintf("WITH _synq_shape_cte AS (%s) SELECT * FROM _synq_shape_cte LIMIT 0", sql)

	collector, ctx := querystats.Start(ctx)
	defer collector.Finish()

	rows, err := db.QueryRows(ctx, wrappedSQL)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	result := make([]*scrapper.QueryShapeColumn, len(columnTypes))
	for i, ct := range columnTypes {
		result[i] = &scrapper.QueryShapeColumn{
			Name:       ct.Name(),
			NativeType: ct.DatabaseTypeName(),
			Position:   int32(i + 1),
		}
	}

	return result, nil
}
