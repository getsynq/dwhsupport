package trino

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/scrapper"
)

type trinoCatalogRow struct {
	CatalogName   string `db:"catalog_name"`
	ConnectorId   string `db:"connector_id"`
	ConnectorName string `db:"connector_name"`
}

//go:embed query_databases.sql
var queryDatabasesSQL string

func (e *TrinoScrapper) QueryDatabases(ctx context.Context) ([]*scrapper.DatabaseRow, error) {
	db := e.executor.GetDb()
	return stdsql.QueryMany(ctx, db, queryDatabasesSQL,
		dwhexec.WithPostProcessors(func(row *scrapper.DatabaseRow) (*scrapper.DatabaseRow, error) {
			row.Instance = e.conf.Host
			return row, nil
		}),
	)
}
