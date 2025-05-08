package trino

import (
	"context"
	_ "embed"
	"strings"

	dwhexec "github.com/getsynq/dwhsupport/exec"
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
		res, err := stdsql.QueryMany(ctx, db, catalogQuery,
			dwhexec.WithPostProcessors(func(row *scrapper.TableRow) (*scrapper.TableRow, error) {
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
