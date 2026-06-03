package trino

import (
	"context"
	_ "embed"
	"strings"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_schemas.sql
var querySchemasSQL string

func (e *TrinoScrapper) QuerySchemas(ctx context.Context) ([]*scrapper.SchemaRow, error) {
	db := e.executor.GetDb()
	var out []*scrapper.SchemaRow

	acceptedCatalogs, err := e.acceptedCatalogs(ctx)
	if err != nil {
		return nil, err
	}

	for _, catalog := range acceptedCatalogs {
		query := strings.Replace(querySchemasSQL, "{{catalog}}", catalog.CatalogName, -1)
		query = scope.AppendSchemaScopeConditions(ctx, query, "catalog_name", "schema_name")
		rows, err := stdsql.QueryMany(ctx, db, query,
			dwhexec.WithPostProcessors(func(row *scrapper.SchemaRow) (*scrapper.SchemaRow, error) {
				row.Instance = e.conf.Host
				return row, nil
			}),
		)
		if err != nil {
			if isCatalogUnavailableError(err) {
				logging.GetLogger(ctx).WithField("catalog", catalog.CatalogName).WithError(err).
					Warn("Catalog is no longer available, skipping")
				continue
			}
			return nil, err
		}
		out = append(out, rows...)
	}
	return out, nil
}
