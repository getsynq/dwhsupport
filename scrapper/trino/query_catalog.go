package trino

import (
	"context"
	_ "embed"
	"strings"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
)

//go:embed query_catalog.sql
var queryCatalogSQL string

func (e *TrinoScrapper) QueryCatalog(ctx context.Context) ([]*scrapper.CatalogColumnRow, error) {
	db := e.executor.GetDb()
	var out []*scrapper.CatalogColumnRow

	availableCatalogs, err := e.allAvailableCatalogs.Get()
	if err != nil {
		return nil, err
	}

	for _, catalog := range availableCatalogs {
		if !catalog.IsAccepted {
			continue
		}
		query := strings.Replace(queryCatalogSQL, "{{catalog}}", catalog.CatalogName, -1)

		// Conditionally add table comments JOIN based on feature flag
		if e.conf.FetchTableComments {
			query = strings.Replace(
				query,
				"{{table_comments_join}}",
				"LEFT JOIN system.metadata.table_comments tc ON t.table_catalog = tc.catalog_name AND t.table_schema = tc.schema_name AND t.table_name = tc.table_name",
				-1,
			)
			query = strings.Replace(query, "{{table_comment_expression}}",
				"coalesce(tc.comment, '')", -1)
		} else {
			query = strings.Replace(query, "{{table_comments_join}}", "", -1)
			query = strings.Replace(query, "{{table_comment_expression}}", "''", -1)
		}
		rows, err := stdsql.QueryMany(ctx, db, query,
			dwhexec.WithPostProcessors(func(row *scrapper.CatalogColumnRow) (*scrapper.CatalogColumnRow, error) {
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
