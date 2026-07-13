package fabric

import (
	"context"
	_ "embed"
	"strings"
	"time"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecfabric "github.com/getsynq/dwhsupport/exec/fabric"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/getsynq/dwhsupport/sqldialect"
)

//go:embed query_table_metrics_tables.sql
var queryTableMetricsTablesSql string

// rowCountBatchSize bounds how many per-table COUNT_BIG(*) selects are UNIONed
// into a single statement, keeping any one query's text and plan size sane on
// warehouses with thousands of tables.
const rowCountBatchSize = 50

type fabricTableRef struct {
	Schema string `db:"schema"`
	Table  string `db:"table"`
}

// QueryTableMetrics returns row counts for Fabric Warehouse tables.
//
// Unlike SQL Server, Fabric Warehouse stores data in columnar parquet/Delta and
// exposes no reliable page-based metadata (no sys.allocation_units, no
// STATS_DATE), so there is no cheap catalog source for row counts, byte sizes,
// or last-modified times. Row counts are therefore computed with a batched
// COUNT_BIG(*) over the in-scope base tables; UpdatedAt and SizeBytes are left
// nil because Fabric surfaces no dependable value for them.
//
// lastMetricsFetchTime is ignored: Fabric provides no per-table modification
// timestamp to drive an incremental fetch, so every call recomputes counts.
func (e *FabricScrapper) QueryTableMetrics(ctx context.Context, lastMetricsFetchTime time.Time) ([]*scrapper.TableMetricsRow, error) {
	listSql := scope.AppendScopeConditions(ctx, queryTableMetricsTablesSql, "", "s.name", "t.name")
	refs, err := dwhexecfabric.NewQuerier[fabricTableRef](e.executor).QueryMany(ctx, listSql)
	if err != nil {
		return nil, err
	}
	if len(refs) == 0 {
		return nil, nil
	}

	var out []*scrapper.TableMetricsRow
	for start := 0; start < len(refs); start += rowCountBatchSize {
		end := start + rowCountBatchSize
		if end > len(refs) {
			end = len(refs)
		}

		rows, err := dwhexecfabric.NewQuerier[scrapper.TableMetricsRow](e.executor).QueryMany(ctx, buildRowCountUnion(refs[start:end]),
			dwhexec.WithPostProcessors(func(row *scrapper.TableMetricsRow) (*scrapper.TableMetricsRow, error) {
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

// buildRowCountUnion assembles a single UNION ALL statement of per-table
// COUNT_BIG(*) selects. Schema/table names are quoted with brackets for the FROM
// clause and emitted as escaped string literals for the projected columns so the
// result carries the identity of each table without a correlated metadata join.
func buildRowCountUnion(refs []*fabricTableRef) string {
	var b strings.Builder
	for i, r := range refs {
		if i > 0 {
			b.WriteString("\nUNION ALL\n")
		}
		fqn := sqldialect.MSSQLQuoteIdentifier(r.Schema) + "." + sqldialect.MSSQLQuoteIdentifier(r.Table)
		b.WriteString("SELECT DB_NAME() AS [database], ")
		b.WriteString(sqlStringLiteral(r.Schema))
		b.WriteString(" AS [schema], ")
		b.WriteString(sqlStringLiteral(r.Table))
		b.WriteString(" AS [table], COUNT_BIG(*) AS row_count FROM ")
		b.WriteString(fqn)
	}
	return b.String()
}

func sqlStringLiteral(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}
