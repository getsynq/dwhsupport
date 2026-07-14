package fabric

import (
	"context"
	"strings"
	"sync"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecfabric "github.com/getsynq/dwhsupport/exec/fabric"
	"github.com/getsynq/dwhsupport/sqldialect"
	"golang.org/x/sync/errgroup"
)

// dbPlaceholder marks, in the embedded metadata SQL, where the bracket-quoted
// database name is injected so catalog views are read cross-database via
// three-part names ([db].sys.tables, [db].INFORMATION_SCHEMA.*). The workspace
// SQL endpoint is shared across all its warehouses/lakehouses, so a single
// connection reads every database this way.
const dbPlaceholder = "{{DB}}"

// maxDatabaseConcurrency bounds concurrent per-database queries, matching the
// Snowflake scrapper's fan-out limit.
const maxDatabaseConcurrency = 4

func expandDatabase(sqlTmpl, database string) string {
	return strings.ReplaceAll(sqlTmpl, dbPlaceholder, sqldialect.MSSQLQuoteIdentifier(database))
}

// queryEachDatabase runs buildSQL(database) against every in-scope workspace
// database concurrently and aggregates the rows. setIdentity stamps the database
// and instance onto each row: the per-database catalog queries deliberately do
// not project DB_NAME() (which would return the connection's entry-point
// database, not the one being read cross-database), so identity is set in Go.
func queryEachDatabase[T any](
	ctx context.Context,
	e *FabricScrapper,
	buildSQL func(database string) string,
	setIdentity func(row *T, database string),
) ([]*T, error) {
	databases, err := e.GetDatabasesToQuery(ctx)
	if err != nil {
		return nil, err
	}

	var (
		mu  sync.Mutex
		out []*T
	)
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxDatabaseConcurrency)
	for _, database := range databases {
		database := database
		g.Go(func() error {
			rows, err := dwhexecfabric.NewQuerier[T](e.executor).QueryMany(gctx, buildSQL(database),
				dwhexec.WithPostProcessors(func(row *T) (*T, error) {
					setIdentity(row, database)
					return row, nil
				}),
			)
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
