package trino

import (
	"context"
	_ "embed"
	"strings"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/scrapper"
)

//go:embed query_sql_definitions.sql
var querySqlDefinitionsSQL string

func (e *TrinoScrapper) QuerySqlDefinitions(ctx context.Context) ([]*scrapper.SqlDefinitionRow, error) {
	query := querySqlDefinitionsSQL
	db := e.executor.GetDb()
	var out []*scrapper.SqlDefinitionRow

	availableCatalogs, err := e.allAvailableCatalogs.Get()
	if err != nil {
		return nil, err
	}

	for _, catalog := range availableCatalogs {
		if !catalog.IsAccepted {
			continue
		}
		catalogQuery := strings.Replace(query, "{{catalog}}", catalog.CatalogName, -1)
		res, err := stdsql.QueryMany(ctx, db, catalogQuery,
			dwhexec.WithPostProcessors(func(row *scrapper.SqlDefinitionRow) (*scrapper.SqlDefinitionRow, error) {
				row.Instance = e.conf.Host
				return row, nil
			}),
		)
		if err != nil {
			return nil, err
		}
		out = append(out, res...)
	}
	return out, nil
}
