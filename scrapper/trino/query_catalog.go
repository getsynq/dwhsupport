package trino

import (
	"context"
	_ "embed"
	"strings"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
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
		rows, err := stdsql.QueryMany(ctx, db, query,
			dwhexec.WithPostProcessors(func(row *scrapper.CatalogColumnRow) (*scrapper.CatalogColumnRow, error) {
				row.Instance = e.conf.Host
				return row, nil
			}),
		)
		if err != nil {
			return nil, err
		}
		out = append(out, rows...)
	}
	return out, nil
}
