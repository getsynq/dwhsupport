package databricks

import (
	"context"
	"sync"

	servicecatalog "github.com/databricks/databricks-sdk-go/service/catalog"
	dwhexecdatabricks "github.com/getsynq/dwhsupport/exec/databricks"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

// unityCatalogWalk reads Unity Catalog the way every query in this package does: the
// workspace's catalogs, each catalog's schemas, and each schema's tables, keeping to
// the ignored catalogs and the scope.
//
// Each catalog's schemas and each schema's tables are a listing of their own, so a
// listing Databricks refuses costs only what it lists. A caller that asked with
// scrapper.WithIncompleteResults gets the rest of the workspace and an omission for
// failedPart naming the catalog or schema left out. One that did not keeps the
// behaviour it had: the first failed listing fails the call. Either way a dropped
// catalog or schema is skipped quietly (see vanished), and these still fail the call:
// the catalog listing itself, a done context, and a rate limit, which the pacing has
// already waited out by the time it surfaces, so the next listing would be refused
// too. A walk in which no listing answered returns its first failure, since there is
// nothing partial about reading nothing.
type unityCatalogWalk struct {
	e                *DatabricksScrapper
	scope            *scope.ScopeFilter
	failedPart       scrapper.OmittedPart
	acceptIncomplete bool

	mu        sync.Mutex
	omissions []scrapper.Omission
	firstErr  error
	read      bool
}

func (e *DatabricksScrapper) walkUnityCatalog(ctx context.Context, failedPart scrapper.OmittedPart) *unityCatalogWalk {
	return &unityCatalogWalk{
		e:                e,
		scope:            e.effectiveScope(ctx),
		failedPart:       failedPart,
		acceptIncomplete: scrapper.IncompleteResultsAccepted(ctx),
	}
}

// forEachCatalog calls visit with the accepted schemas of each accepted catalog. A
// visit that reads what it was given marks it with markRead.
func (w *unityCatalogWalk) forEachCatalog(
	ctx context.Context,
	visit func(catalog servicecatalog.CatalogInfo, schemas []servicecatalog.SchemaInfo) error,
) error {
	log := logging.GetLogger(ctx)
	catalogs, err := w.e.listCatalogs(ctx)
	if err != nil {
		return err
	}
	for _, catalogInfo := range catalogs {
		if w.e.isIgnoredCatalog(catalogInfo) {
			continue
		}
		if !w.scope.IsDatabaseAccepted(catalogInfo.Name) {
			log.Infof("catalog %s excluded by scope filter", catalogInfo.Name)
			continue
		}

		schemas, err := w.e.listSchemas(ctx, catalogInfo.Name)
		if err != nil {
			if err := w.leaveOut(ctx, scrapper.Omission{Database: catalogInfo.Name, Part: w.failedPart, Err: err}); err != nil {
				return err
			}
			continue
		}
		var accepted []servicecatalog.SchemaInfo
		for _, schemaInfo := range schemas {
			if schemaInfo.Name == "information_schema" {
				continue
			}
			if !w.scope.IsSchemaAccepted(catalogInfo.Name, schemaInfo.Name) {
				log.Infof("schema %s.%s excluded by scope filter", catalogInfo.Name, schemaInfo.Name)
				continue
			}
			accepted = append(accepted, schemaInfo)
		}
		if err := visit(catalogInfo, accepted); err != nil {
			return err
		}
	}
	return nil
}

// forEachSchema calls visit with the accepted tables of each accepted schema. request
// names how much of each table's metadata the listing carries.
func (w *unityCatalogWalk) forEachSchema(
	ctx context.Context,
	request servicecatalog.ListTablesRequest,
	visit func(schema servicecatalog.SchemaInfo, tables []servicecatalog.TableInfo) error,
) error {
	log := logging.GetLogger(ctx)
	return w.forEachCatalog(ctx, func(catalogInfo servicecatalog.CatalogInfo, schemas []servicecatalog.SchemaInfo) error {
		if len(schemas) == 0 {
			// The catalog was read, and it has nothing to list.
			w.markRead()
			return nil
		}
		for _, schemaInfo := range schemas {
			request.CatalogName, request.SchemaName = catalogInfo.Name, schemaInfo.Name
			tables, err := w.e.listTables(ctx, request)
			if err != nil {
				omission := scrapper.Omission{Database: catalogInfo.Name, Schema: schemaInfo.Name, Part: w.failedPart, Err: err}
				if err := w.leaveOut(ctx, omission); err != nil {
					return err
				}
				continue
			}
			w.markRead()
			accepted := tables[:0:0]
			for _, tableInfo := range tables {
				if !w.scope.IsObjectAccepted(catalogInfo.Name, schemaInfo.Name, tableInfo.Name) {
					log.Infof("table %s.%s.%s excluded by scope filter", catalogInfo.Name, schemaInfo.Name, tableInfo.Name)
					continue
				}
				accepted = append(accepted, tableInfo)
			}
			if err := visit(schemaInfo, accepted); err != nil {
				return err
			}
		}
		return nil
	})
}

// markRead records that a listing answered, so the walk did not read nothing.
func (w *unityCatalogWalk) markRead() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.read = true
}

// leaveOut records what a failed read left out, or returns the error when the call
// has to fail instead. Safe for concurrent use.
func (w *unityCatalogWalk) leaveOut(ctx context.Context, omission scrapper.Omission) error {
	err := omission.Err
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if !w.acceptIncomplete || dwhexecdatabricks.IsRateLimitError(err) {
		return err
	}
	logging.GetLogger(ctx).WithField("scope", omission.Scope()).WithError(err).
		Warnf("Could not read the %s of %s, leaving it out", omission.Part, omission.Scope())

	w.mu.Lock()
	defer w.mu.Unlock()
	w.omissions = append(w.omissions, omission)
	if w.firstErr == nil {
		w.firstErr = err
	}
	return nil
}

// result is the error a finished walk returns beside its rows: nil, an
// *scrapper.IncompleteError naming what was left out, or the first failure when no
// listing answered.
func (w *unityCatalogWalk) result() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.read && w.firstErr != nil {
		return w.firstErr
	}
	return scrapper.Incomplete(w.omissions)
}
