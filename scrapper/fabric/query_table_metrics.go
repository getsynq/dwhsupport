package fabric

import (
	"context"
	_ "embed"
	"strings"
	"sync"
	"time"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecfabric "github.com/getsynq/dwhsupport/exec/fabric"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/getsynq/dwhsupport/sqldialect"
	"golang.org/x/sync/errgroup"
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
// This is intentionally NOT a metadata-level query: Fabric Warehouse exposes no
// working row-count metadata source. Verified live against the engine
// ("Azure SQL Data Warehouse 12.0.2000.8"):
//   - sys.dm_db_partition_stats — "DMV is not supported"
//   - sys.dm_pdw_nodes_db_partition_stats — invalid object name
//   - sys.partitions.rows — NULL for user (columnar) tables
//   - no sys.allocation_units / STATS_DATE (columnar parquet/Delta storage)
//
// The official microsoft/dbt-fabric adapter's catalog macro likewise reads no
// row-count/size metadata (schema only) — there is no metadata source to read.
//
// Row counts are therefore computed with a batched COUNT_BIG(*) over the
// in-scope base tables. COUNT is comparatively cheap on Fabric's columnar
// engine, but callers should still schedule this at a sane cadence. UpdatedAt
// and SizeBytes are left nil because Fabric surfaces no dependable value.
//
// lastMetricsFetchTime is ignored: Fabric provides no per-table modification
// timestamp to drive an incremental fetch, so every call recomputes counts.
//
// Workspace-centric: the in-scope databases are iterated concurrently, and each
// table is counted with a three-part [db].[schema].[table] name.
func (e *FabricScrapper) QueryTableMetrics(ctx context.Context, lastMetricsFetchTime time.Time) ([]*scrapper.TableMetricsRow, error) {
	ctx = e.withEffectiveScope(ctx)
	databases, err := e.GetDatabasesToQuery(ctx)
	if err != nil {
		return nil, err
	}

	var (
		mu  sync.Mutex
		out []*scrapper.TableMetricsRow
	)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxDatabaseConcurrency)
	for _, database := range databases {
		database := database
		g.Go(func() error {
			rows, err := e.tableMetricsForDatabase(gctx, database)
			if err != nil {
				return err
			}
			mu.Lock()
			out = append(out, rows...)
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return out, nil
}

func (e *FabricScrapper) tableMetricsForDatabase(ctx context.Context, database string) ([]*scrapper.TableMetricsRow, error) {
	listSql := expandDatabase(scope.AppendScopeConditions(ctx, queryTableMetricsTablesSql, "", "s.name", "t.name"), database)
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

		rows, err := dwhexecfabric.NewQuerier[scrapper.TableMetricsRow](e.executor).QueryMany(ctx, buildRowCountUnion(database, refs[start:end]),
			dwhexec.WithPostProcessors(func(row *scrapper.TableMetricsRow) (*scrapper.TableMetricsRow, error) {
				row.Instance = e.conf.Host
				row.Database = database
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
// COUNT_BIG(*) selects for one database. The FROM clause uses a three-part
// [db].[schema].[table] name so counts are read cross-database, and schema/table
// are emitted as escaped literals so each row carries its identity without a
// correlated metadata join (Database/Instance are stamped in Go).
func buildRowCountUnion(database string, refs []*fabricTableRef) string {
	dbPrefix := sqldialect.MSSQLQuoteIdentifier(database)
	var b strings.Builder
	for i, r := range refs {
		if i > 0 {
			b.WriteString("\nUNION ALL\n")
		}
		fqn := dbPrefix + "." + sqldialect.MSSQLQuoteIdentifier(r.Schema) + "." + sqldialect.MSSQLQuoteIdentifier(r.Table)
		b.WriteString("SELECT ")
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
