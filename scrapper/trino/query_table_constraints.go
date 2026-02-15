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

//go:embed query_table_constraints.sql
var queryTableConstraintsSql string

func (e *TrinoScrapper) QueryTableConstraints(ctx context.Context) ([]*scrapper.TableConstraintRow, error) {
	var out []*scrapper.TableConstraintRow

	availableCatalogs, err := e.allAvailableCatalogs.Get()
	if err != nil {
		return nil, err
	}

	for _, catalog := range availableCatalogs {
		if !catalog.IsAccepted {
			continue
		}
		res, err := e.queryTableConstraintsForCatalog(ctx, catalog.CatalogName)
		if err != nil {
			return nil, err
		}
		out = append(out, res...)
	}
	return out, nil
}

func (e *TrinoScrapper) queryTableConstraintsForCatalog(ctx context.Context, catalogName string) ([]*scrapper.TableConstraintRow, error) {
	query := strings.ReplaceAll(queryTableConstraintsSql, "{{catalog}}", catalogName)

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
			logging.GetLogger(ctx).WithField("catalog", catalogName).WithError(err).
				Warn("Catalog is no longer available, skipping")
			return nil, nil
		}
		return nil, err
	}
	return res, nil
}
