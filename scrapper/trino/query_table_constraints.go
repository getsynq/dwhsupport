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

func (e *TrinoScrapper) QueryTableConstraints(ctx context.Context, schema string, table string) ([]*scrapper.TableConstraintRow, error) {
	availableCatalogs, err := e.allAvailableCatalogs.Get()
	if err != nil {
		return nil, err
	}

	for _, catalog := range availableCatalogs {
		if !catalog.IsAccepted {
			continue
		}
		rows, err := e.queryTableConstraintsInCatalog(ctx, catalog.CatalogName, schema, table)
		if err != nil {
			continue
		}
		if len(rows) > 0 {
			return rows, nil
		}
	}

	return nil, nil
}

func (e *TrinoScrapper) queryTableConstraintsInCatalog(
	ctx context.Context,
	catalogName string,
	schema string,
	table string,
) ([]*scrapper.TableConstraintRow, error) {
	query := strings.ReplaceAll(queryTableConstraintsSql, "{{catalog}}", catalogName)

	rows, err := stdsql.QueryMany[scrapper.TableConstraintRow](ctx, e.executor.GetDb(), query,
		dwhexec.WithArgs[scrapper.TableConstraintRow](schema, table),
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

	return rows, nil
}
