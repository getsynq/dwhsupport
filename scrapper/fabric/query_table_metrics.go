package fabric

import (
	"context"
	_ "embed"
	"strings"
	"sync"
	"time"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecfabric "github.com/getsynq/dwhsupport/exec/fabric"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/pkg/errors"
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
// engine, but callers should still schedule this at a sane cadence. SizeBytes
// is left nil because Fabric surfaces no dependable value.
//
// The SQL surface has no time of the last change either. With
// FetchLastLoadTime, UpdatedAt comes from the table's Delta log in OneLake
// (lastChanges). Without it, or if the log cannot be read, UpdatedAt is nil
// and the row count stays.
//
// lastMetricsFetchTime is ignored: the row count has no cheaper read to skip,
// so every call recomputes counts.
//
// Workspace-centric: the in-scope databases are iterated concurrently, and each
// table is counted with a three-part [db].[schema].[table] name. A database
// whose listing or any count fails is left out whole (readDatabases).
func (e *FabricScrapper) QueryTableMetrics(ctx context.Context, lastMetricsFetchTime time.Time) ([]*scrapper.TableMetricsRow, error) {
	ctx = e.withEffectiveScope(ctx)
	databases, err := e.GetDatabasesToQuery(ctx)
	if err != nil {
		return nil, err
	}

	return readDatabases(ctx, databases, scrapper.OmittedMetrics, e.tableMetricsForDatabase)
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

	// The Delta log reads need only the table names, so they run while the
	// tables are counted.
	var changes map[fabricTableRef]*time.Time
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		var err error
		changes, err = e.lastChanges(gctx, database, refs)
		return err
	})
	var out []*scrapper.TableMetricsRow
	g.Go(func() error {
		var err error
		out, err = e.countRows(gctx, database, refs)
		return err
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}
	for _, row := range out {
		row.UpdatedAt = changes[fabricTableRef{Schema: row.Schema, Table: row.Table}]
	}
	return out, nil
}

// countRows counts the rows of the tables, rowCountBatchSize tables in each query.
func (e *FabricScrapper) countRows(ctx context.Context, database string, refs []*fabricTableRef) ([]*scrapper.TableMetricsRow, error) {
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

// lastChanges returns the time of the last commit that changed data of each
// table, if FetchLastLoadTime is on. A table with no Delta log, and a read that
// fails, has no entry. The failures are logged once for the database. Only a
// cancelled context fails the call.
func (e *FabricScrapper) lastChanges(ctx context.Context, database string, refs []*fabricTableRef) (map[fabricTableRef]*time.Time, error) {
	if !e.conf.FetchLastLoadTime {
		return nil, nil
	}
	log := logging.GetLogger(ctx).WithField("database", database)
	reader, err := e.deltaLog.Get()
	if err != nil {
		log.WithError(err).Debug("No OneLake access, tables have no last change")
		return nil, nil
	}
	item, ok, err := reader.itemOf(ctx, database)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		log.WithError(err).Warn("Workspace items could not be listed, tables have no last change")
		return nil, nil
	}
	if !ok {
		log.Debug("No single storage item has the name of the database, tables have no last change")
		return nil, nil
	}
	var mu sync.Mutex
	changes := map[fabricTableRef]*time.Time{}
	var failed int
	var firstErr error
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxDeltaLogConcurrency)
	for _, ref := range refs {
		g.Go(func() error {
			at, err := reader.lastChange(gctx, item.ID, tableDirs(item.HasSchemas(), ref.Schema, ref.Table))
			if errors.Is(err, errNoDeltaLog) {
				// A shortcut to files that are not a Delta table, for example.
				return nil
			}
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				mu.Lock()
				failed++
				if firstErr == nil {
					firstErr = errors.Wrapf(err, "%s.%s", ref.Schema, ref.Table)
				}
				mu.Unlock()
				return nil
			}
			mu.Lock()
			changes[*ref] = at
			mu.Unlock()
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	if failed > 0 {
		log.WithError(firstErr).WithField("tables", failed).Warn("Delta log could not be read, those tables have no last change")
	}
	return changes, nil
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
