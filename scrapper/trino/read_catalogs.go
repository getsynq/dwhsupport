package trino

import (
	"context"

	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
)

// catalogReader reads one catalog. Its omissions describe what it left out of a
// result it still returns; its error means it read nothing of the catalog.
type catalogReader[T any] func(ctx context.Context, catalog *TrinoCatalog) ([]*T, []scrapper.Omission, error)

// readCatalogs runs read over each catalog in turn, so a catalog Trino cannot
// read costs only that catalog. Trino already leaves out a single table whose
// metadata is missing or corrupt, so what fails a catalog's query is something
// every table in it shares, usually its object storage, and the catalog is the
// smallest scope worth reading on its own.
//
// A caller that asked with scrapper.WithIncompleteResults gets the other
// catalogs' rows and an *IncompleteError naming the failed catalog as omitted
// for failedPart. One that did not keeps the behaviour it had: the first
// failure fails the call, except for a catalog Trino reports as gone or whose
// connector cannot reach its database (isCatalogUnavailableError), which is
// skipped. A catalog Trino failed to list is not skipped, since its tables would
// be published without columns. Either way, a call in which every catalog failed
// returns the first failure, since there is nothing partial about reading nothing.
func readCatalogs[T any](
	ctx context.Context,
	catalogs []*TrinoCatalog,
	failedPart scrapper.OmittedPart,
	read catalogReader[T],
) ([]*T, error) {
	acceptIncomplete := scrapper.IncompleteResultsAccepted(ctx)

	var out []*T
	var omissions []scrapper.Omission
	var firstErr error
	failed := 0

	for _, catalog := range catalogs {
		log := logging.GetLogger(ctx).WithField("catalog", catalog.CatalogName)
		rows, catalogOmissions, err := read(ctx, catalog)
		if err != nil {
			if ctx.Err() != nil {
				return nil, err
			}
			if !acceptIncomplete {
				if isCatalogUnavailableError(err) {
					log.WithError(err).Warn("Catalog is no longer available, skipping")
					continue
				}
				return nil, err
			}
			log.WithError(err).Warn("Catalog could not be read, leaving it out")
			failed++
			if firstErr == nil {
				firstErr = err
			}
			omissions = append(omissions, scrapper.Omission{Database: catalog.CatalogName, Part: failedPart, Err: err})
			continue
		}
		if len(catalogOmissions) > 0 && !acceptIncomplete {
			return nil, catalogOmissions[0].Err
		}
		out = append(out, rows...)
		omissions = append(omissions, catalogOmissions...)
	}

	if failed > 0 && failed == len(catalogs) {
		return nil, firstErr
	}
	return out, scrapper.Incomplete(omissions)
}

// withoutCommentsOnFailure runs query with the table comments join and, when
// Trino fails it, once more without. system.metadata.table_comments loads every
// table's metadata, which the bare information_schema listing does not, so a
// catalog whose storage cannot be read is still listed, without descriptions.
// Only a caller accepting incomplete results is offered the second attempt; one
// that is not gets the failure, as it always did.
func (e *TrinoScrapper) withoutCommentsOnFailure(
	ctx context.Context,
	catalogName string,
	query func(withComments bool) error,
) ([]scrapper.Omission, error) {
	err := query(e.conf.FetchTableComments)
	if err == nil || !e.conf.FetchTableComments || !scrapper.IncompleteResultsAccepted(ctx) ||
		ctx.Err() != nil || isCatalogUnavailableError(err) {
		return nil, err
	}
	logging.GetLogger(ctx).WithField("catalog", catalogName).WithError(err).
		Warn("Reading table comments failed, reading the catalog without them")
	if retryErr := query(false); retryErr != nil {
		return nil, err
	}
	return []scrapper.Omission{{Database: catalogName, Part: scrapper.OmittedDescriptions, Err: err}}, nil
}

// noCommentExpression is the description of a table read without its comment:
// empty when comments are not fetched at all, unknown when they were asked for
// and could not be read, so a reader can tell an empty comment from a missing one.
func noCommentExpression(commentsRequested bool) string {
	if commentsRequested {
		return "CAST(NULL AS varchar)"
	}
	return "''"
}

// isMySQLViewComment is true for the comment of a view in a catalog backed by
// MySQL or MariaDB, which report the comment of every view as the text "VIEW".
// Their Trino connectors list a view as a BASE TABLE, so the comment is the only
// sign of it, and a table whose comment is exactly "VIEW" loses it too.
func isMySQLViewComment(catalog *TrinoCatalog, comment *string) bool {
	switch catalog.ConnectorName {
	case "mysql", "mariadb":
		return comment != nil && *comment == "VIEW"
	}
	return false
}
