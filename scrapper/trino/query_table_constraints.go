package trino

import (
	"context"
	_ "embed"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

//go:embed query_table_constraints.sql
var queryTableConstraintsSql string

func (e *TrinoScrapper) QueryTableConstraints(ctx context.Context) ([]*scrapper.TableConstraintRow, error) {
	var out []*scrapper.TableConstraintRow

	acceptedCatalogs, err := e.acceptedCatalogs(ctx)
	if err != nil {
		return nil, err
	}

	for _, catalog := range acceptedCatalogs {
		res, err := e.queryTableConstraintsForCatalog(ctx, catalog)
		if err != nil {
			return nil, err
		}
		out = append(out, res...)
	}
	return out, nil
}

func (e *TrinoScrapper) queryTableConstraintsForCatalog(ctx context.Context, catalog *TrinoCatalog) ([]*scrapper.TableConstraintRow, error) {
	query := withCatalog(queryTableConstraintsSql, catalog)
	query = scope.AppendScopeConditions(ctx, query, "", "\"schema\"", "\"table\"")

	res, err := stdsql.QueryMany[scrapper.TableConstraintRow](ctx, e.executor.GetDb(), query,
		dwhexec.WithPostProcessors(func(row *scrapper.TableConstraintRow) (*scrapper.TableConstraintRow, error) {
			row.Instance = e.conf.Host
			if row.ConstraintType == "UNIQUE" {
				row.ConstraintType = scrapper.ConstraintTypeUniqueIndex
			}
			return row, nil
		}),
	)
	if err != nil {
		if isCatalogUnavailableError(err) {
			logging.GetLogger(ctx).WithField("catalog", catalog.CatalogName).WithError(err).
				Warn("Catalog is no longer available, skipping")
			return nil, nil
		}
		if isTableNotFoundError(err) {
			logging.GetLogger(ctx).WithField("catalog", catalog.CatalogName).
				Debug("information_schema.table_constraints not available for catalog, skipping")
			return nil, nil
		}
		return nil, err
	}
	return res, nil
}
