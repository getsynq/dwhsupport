package snowflake

import (
	"context"
	"slices"
	"strings"
	"sync"

	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"golang.org/x/sync/errgroup"
)

// databaseReader reads one database. Its error means it read nothing of it.
type databaseReader[T any] func(ctx context.Context, database string) ([]*T, error)

// readDatabases runs read over each database, at most limit at a time, so a
// database Snowflake cannot read costs only that database.
//
// A caller that asked with scrapper.WithIncompleteResults gets the other
// databases' rows and an *IncompleteError naming each failed database as
// omitted for failedPart. That includes an imported database whose publisher
// stopped sharing it (003030): Snowflake still lists it, so it is configured
// and in SHOW DATABASES, and reading its absence as its removal would delete
// everything known about it, only to recreate it without its history when the
// share comes back. A share removed on purpose is taken out of the
// configuration or dropped, and then is not read at all.
//
// One that did not ask keeps the behaviour it had: the first failure fails the
// call, except for an unavailable share, which is skipped. Either way, a call
// in which every database failed returns the failure of the first database by name, since there is
// nothing partial about reading nothing.
func readDatabases[T any](
	ctx context.Context,
	databases []string,
	limit int,
	failedPart scrapper.OmittedPart,
	read databaseReader[T],
) ([]*T, error) {
	acceptIncomplete := scrapper.IncompleteResultsAccepted(ctx)

	var m sync.Mutex
	var out []*T
	var omissions []scrapper.Omission

	g, groupCtx := errgroup.WithContext(ctx)
	g.SetLimit(limit)
	for _, database := range databases {
		if groupCtx.Err() != nil {
			break
		}
		g.Go(func() error {
			rows, err := read(groupCtx, database)
			if err != nil {
				if groupCtx.Err() != nil {
					return err
				}
				log := logging.GetLogger(groupCtx).WithField("database", database).WithError(err)
				if !acceptIncomplete {
					if isSharedDatabaseUnavailableError(err) {
						log.Warn("Shared database is no longer available, skipping")
						return nil
					}
					return err
				}
				log.Warn("Database could not be read, leaving it out")
				m.Lock()
				defer m.Unlock()
				omissions = append(omissions, scrapper.Omission{Database: database, Part: failedPart, Err: err})
				return nil
			}
			m.Lock()
			defer m.Unlock()
			out = append(out, rows...)
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	slices.SortFunc(omissions, func(a, b scrapper.Omission) int { return strings.Compare(a.Database, b.Database) })
	if len(databases) > 0 && len(omissions) == len(databases) {
		return nil, omissions[0].Err
	}
	return out, scrapper.Incomplete(omissions)
}
