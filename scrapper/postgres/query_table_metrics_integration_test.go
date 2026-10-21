package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/testenv"
	"github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// relationStats is what the catalog and the cumulative statistics say about a relation, read
// beside QueryTableMetrics so the test can tell which branch of the row count applies to it.
type relationStats struct {
	Schema       string  `db:"schema"`
	Table        string  `db:"table"`
	Kind         string  `db:"kind"`
	Reltuples    float64 `db:"reltuples"`
	Relpages     int64   `db:"relpages"`
	HeapBytes    int64   `db:"heap_bytes"`
	LiveTuples   *int64  `db:"live_tuples"`
	CanBeCounted bool    `db:"can_be_counted"`
}

const relationStatsSql = `
select sch.nspname                          as "schema",
       tbl.relname                          as "table",
       tbl.relkind::text                    as "kind",
       tbl.reltuples                        as "reltuples",
       tbl.relpages                         as "relpages",
       pg_catalog.pg_relation_size(tbl.oid) as "heap_bytes",
       st.n_live_tup                        as "live_tuples",
       has_table_privilege(tbl.oid, 'SELECT') and tbl.relkind in ('r', 'm', 'p') as "can_be_counted"
from pg_catalog.pg_namespace sch
         join pg_catalog.pg_class tbl on tbl.relnamespace = sch.oid
         left join pg_catalog.pg_stat_user_tables st on st.relid = tbl.oid
where not pg_is_other_temp_schema(sch.oid)
  and tbl.relpersistence in ('p', 'u')
  and tbl.relkind in ('r', 'f', 'p', 'm')
  and sch.nspname not in ('pg_catalog', 'information_schema')
  and sch.nspname not like 'pg_toast%'
  and sch.nspname not like 'pg_temp%'`

// TestPostgresTableMetricsCoversEveryRelation reads the dwhtesting instance, whose seed tables are
// too small for autovacuum to analyze them (reltuples = -1 since Postgres 14), and whose cumulative
// statistics are lost on every restart (n_live_tup = 0 for every table). The test role may not
// create tables, so each case is checked on whichever relations the instance has in that state.
func TestPostgresTableMetricsCoversEveryRelation(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping Postgres integration tests in CI")
	}
	if testenv.EnvOrDefault("POSTGRES_HOST", "") == "" {
		t.Skip("POSTGRES_HOST env var not set")
	}
	ctx := context.Background()
	sc, err := newPostgresScrapperFromEnv(ctx)
	if err != nil {
		t.Skipf("Could not connect to Postgres: %v", err)
	}
	defer sc.Close()

	var relations []relationStats
	require.NoError(t, sc.executor.GetDb().SelectContext(ctx, &relations, relationStatsSql))
	require.NotEmpty(t, relations)

	metrics, err := sc.QueryTableMetrics(ctx, time.Time{})
	require.NoError(t, err)
	rowCounts := map[string]*int64{}
	for _, m := range metrics {
		rowCounts[m.Schema+"."+m.Table] = m.RowCount
	}

	seen := map[string]int{}
	for _, rel := range relations {
		name := rel.Schema + "." + rel.Table
		rowCount, listed := rowCounts[name]
		if !assert.Truef(t, listed, "%s (reltuples %v) has no metrics row", name, rel.Reltuples) {
			continue
		}

		switch {
		case rel.Reltuples >= 0 && rel.Relpages > 0:
			// Analyzed or vacuumed: the planner's estimate, which a lost n_live_tup does not zero.
			seen["analyzed"]++
			if !assert.NotNilf(t, rowCount, "%s", name) {
				continue
			}
			if rel.Reltuples > 0 && rel.HeapBytes > 0 {
				assert.Positivef(t, *rowCount, "%s was analyzed with %v rows", name, rel.Reltuples)
			}
			if rel.CanBeCounted {
				var actual int64
				require.NoError(t, sc.executor.GetDb().GetContext(ctx, &actual,
					`select count(*) from `+pq.QuoteIdentifier(rel.Schema)+"."+pq.QuoteIdentifier(rel.Table)))
				assert.InDeltaf(t, actual, *rowCount, float64(actual)/2+1, "%s", name)
			}
		case (rel.Kind == "r" || rel.Kind == "m") && rel.HeapBytes == 0:
			seen["empty heap"]++
			if assert.NotNilf(t, rowCount, "%s", name) {
				assert.Zerof(t, *rowCount, "%s has no heap", name)
			}
		case rel.Reltuples >= 0 && (rel.Kind == "p" || rel.Kind == "f"):
			// No heap of its own, so relpages stays 0; ANALYZE sets reltuples.
			seen["analyzed, no heap of its own"]++
			if assert.NotNilf(t, rowCount, "%s", name) {
				assert.Equalf(t, int64(rel.Reltuples), *rowCount, "%s", name)
			}
		case rel.LiveTuples == nil || *rel.LiveTuples == 0:
			// Never analyzed and the statistics know no rows of it: unknown, never a made-up 0. A
			// partitioned table never analyzed lands here too, its rows are counted in its partitions.
			seen["never analyzed, statistics lost"]++
			assert.Nilf(t, rowCount, "%s was never analyzed and n_live_tup knows no rows", name)
		default:
			seen["never analyzed, statistics kept"]++
			if assert.NotNilf(t, rowCount, "%s", name) {
				assert.Equalf(t, *rel.LiveTuples, *rowCount, "%s", name)
			}
		}
	}
	t.Logf("relations per case: %v", seen)
}
