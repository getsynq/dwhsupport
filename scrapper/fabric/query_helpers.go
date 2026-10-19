package fabric

import (
	"context"
	"strings"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecfabric "github.com/getsynq/dwhsupport/exec/fabric"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
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

// wholeListing is the failedPart of a listing that fails as a whole when one
// database fails, whatever the caller accepts. Schemas and constraints are such
// listings: the wrappers do not forward an incomplete result for them.
const wholeListing scrapper.OmittedPart = ""

// queryEachDatabase runs buildSQL(database) against every in-scope workspace
// database and aggregates the rows, as readDatabases does. setIdentity stamps
// the database and instance onto each row: the per-database catalog queries
// deliberately do not project DB_NAME() (which would return the connection's
// entry-point database, not the one being read cross-database), so identity is
// set in Go.
func queryEachDatabase[T any](
	ctx context.Context,
	e *FabricScrapper,
	failedPart scrapper.OmittedPart,
	buildSQL func(database string) string,
	setIdentity func(row *T, database string),
) ([]*T, error) {
	databases, err := e.GetDatabasesToQuery(ctx)
	if err != nil {
		return nil, err
	}
	return readDatabases(ctx, databases, failedPart, func(ctx context.Context, database string) ([]*T, error) {
		return dwhexecfabric.NewQuerier[T](e.executor).QueryMany(ctx, buildSQL(database),
			dwhexec.WithPostProcessors(func(row *T) (*T, error) {
				setIdentity(row, database)
				return row, nil
			}),
		)
	})
}

// databaseReader reads one database. Its error means it read nothing of the
// database.
type databaseReader[T any] func(ctx context.Context, database string) ([]*T, error)

// readDatabases runs read over the databases concurrently, so a database the
// endpoint cannot serve costs only that database. Every database is read
// cross-database through the one workspace endpoint, so what fails a read is
// usually the database itself (paused, being provisioned, its storage
// unreachable), and the database is the smallest scope worth reading on its own.
//
// A caller that asked with scrapper.WithIncompleteResults gets the other
// databases' rows and an *IncompleteError naming each failed database as
// omitted for failedPart. One that did not, or a wholeListing, keeps the
// behaviour it had: the first failure fails the call. Either way a cancelled
// context fails the call, and so does a call in which every database failed,
// with the first failure, since there is nothing partial about reading nothing.
// Rows and omissions come in the order of databases, whichever read finished
// first.
func readDatabases[T any](
	ctx context.Context,
	databases []string,
	failedPart scrapper.OmittedPart,
	read databaseReader[T],
) ([]*T, error) {
	acceptIncomplete := failedPart != wholeListing && scrapper.IncompleteResultsAccepted(ctx)

	type databaseRead struct {
		rows []*T
		err  error
	}
	reads := make([]databaseRead, len(databases))
	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(maxDatabaseConcurrency)
	for i, database := range databases {
		g.Go(func() error {
			rows, err := read(gctx, database)
			if err != nil && (!acceptIncomplete || ctx.Err() != nil) {
				return err
			}
			reads[i] = databaseRead{rows: rows, err: err}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var out []*T
	var omissions []scrapper.Omission
	var firstErr error
	for i, r := range reads {
		if r.err == nil {
			out = append(out, r.rows...)
			continue
		}
		logging.GetLogger(ctx).WithField("database", databases[i]).WithError(r.err).
			Warn("Database could not be read, leaving it out")
		if firstErr == nil {
			firstErr = r.err
		}
		omissions = append(omissions, scrapper.Omission{Database: databases[i], Part: failedPart, Err: r.err})
	}
	if len(databases) > 0 && len(omissions) == len(databases) {
		return nil, firstErr
	}
	return out, scrapper.Incomplete(omissions)
}
