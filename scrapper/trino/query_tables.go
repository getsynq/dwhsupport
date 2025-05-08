package trino

import (
	"context"
	_ "embed"
	"strings"

	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/getsynq/dwhsupport/scrapper"
)

//go:embed query_tables.sql
var queryTablesSQL string

func (e *TrinoScrapper) QueryTables(ctx context.Context) ([]*scrapper.TableRow, error) {
	query := queryTablesSQL
	db := e.executor.GetDb()
	var out []*scrapper.TableRow

	for _, catalog := range e.conf.Catalogs {
		catalogQuery := strings.Replace(query, "{{catalog}}", catalog, -1)
		res, err := stdsql.QueryMany[scrapper.TableRow](ctx, db, catalogQuery)
		if err != nil {
			return nil, err
		}
		out = append(out, res...)
	}
	return out, nil
}
