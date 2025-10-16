package trino

import (
	"context"
	"fmt"
	"strings"
	"time"

	dwhexec "github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	dwhexectrino "github.com/getsynq/dwhsupport/exec/trino"
	"github.com/getsynq/dwhsupport/lazy"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/samber/lo"
)

type TrinoScrapperConf struct {
	*dwhexectrino.TrinoConf
	Catalogs               []string
	UseShowCreateView      bool
	UseShowCreateTable     bool
	FetchMaterializedViews bool
	FetchTableComments     bool
}

var _ scrapper.Scrapper = &TrinoScrapper{}

type TrinoScrapper struct {
	conf                 *TrinoScrapperConf
	executor             *dwhexectrino.TrinoExecutor
	allAvailableCatalogs lazy.Lazy[[]*TrinoCatalog]
}

type TrinoCatalog struct {
	CatalogName   string `db:"catalog_name"   json:"catalog_name"`
	ConnectorId   string `db:"connector_id"   json:"connector_id"`
	ConnectorName string `db:"connector_name" json:"connector_name"`
	IsAccepted    bool   `db:"-"              json:"is_accepted"`
}

func (c *TrinoCatalog) String() string {
	return fmt.Sprintf("%s (%s:%s)", c.CatalogName, c.ConnectorId, c.ConnectorName)
}

func NewTrinoScrapper(ctx context.Context, conf *TrinoScrapperConf) (*TrinoScrapper, error) {
	executor, err := dwhexectrino.NewTrinoExecutor(ctx, conf.TrinoConf)
	if err != nil {
		return nil, err
	}

	allAvailableCatalogs := lazy.New[[]*TrinoCatalog](func() ([]*TrinoCatalog, error) {
		db := executor.GetDb()
		allCatalogs, err := stdsql.QueryMany[TrinoCatalog](ctx, db, "SELECT * FROM system.metadata.catalogs",
			dwhexec.WithPostProcessors[TrinoCatalog](func(row *TrinoCatalog) (*TrinoCatalog, error) {
				row.IsAccepted = lo.Contains(conf.Catalogs, row.CatalogName)
				return row, nil
			}),
		)
		if err != nil {
			return nil, err
		}

		logging.GetLogger(ctx).WithField("all_catalogs", allCatalogs).Info("system.metadata.catalogs")

		return allCatalogs, nil
	})

	return &TrinoScrapper{
		conf:                 conf,
		executor:             executor,
		allAvailableCatalogs: allAvailableCatalogs,
	}, nil
}

func (e *TrinoScrapper) IsPermissionError(err error) bool {
	// TODO: Implement Trino-specific error check
	return false
}

func (e *TrinoScrapper) DialectType() string {
	return "trino"
}

func (e *TrinoScrapper) SqlDialect() sqldialect.Dialect {
	return sqldialect.NewTrinoDialect()
}

func (e *TrinoScrapper) ValidateConfiguration(ctx context.Context) ([]string, error) {

	availableCatalogs, err := e.allAvailableCatalogs.Get()
	if err != nil {
		return nil, err
	}

	existingCatalogs := map[string]bool{}
	for _, catalog := range availableCatalogs {
		existingCatalogs[catalog.CatalogName] = true
	}

	var missingCatalogs []string
	var unavailableCatalogs []string

	for _, catalog := range e.conf.Catalogs {
		if !existingCatalogs[catalog] {
			missingCatalogs = append(missingCatalogs, catalog)
			continue
		}

		// Probe the catalog to check if it's accessible with a 5 second timeout
		// We use a simple query against information_schema which will fail if the catalog is unavailable
		probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		probeQuery := fmt.Sprintf("SELECT 1 FROM %s.information_schema.schemata LIMIT 1", catalog)
		_, probeErr := e.executor.GetDb().QueryContext(probeCtx, probeQuery)
		cancel()

		if probeErr != nil {
			if isCatalogUnavailableError(probeErr) || probeErr == context.DeadlineExceeded {
				logging.GetLogger(ctx).WithField("catalog", catalog).WithError(probeErr).
					Warn("Catalog is no longer available")
				unavailableCatalogs = append(unavailableCatalogs, catalog)
			} else {
				// For other errors, log but don't fail validation
				logging.GetLogger(ctx).WithField("catalog", catalog).WithError(probeErr).
					Debug("Catalog probe query failed")
			}
		}
	}

	var messages []string

	if len(missingCatalogs) > 0 {
		messages = append(messages, fmt.Sprintf("Catalog not found or no permissions to access: %s", strings.Join(missingCatalogs, ", ")))
	}

	if len(unavailableCatalogs) > 0 {
		messages = append(
			messages,
			fmt.Sprintf(
				"Catalog(s) no longer available and will be skipped during data extraction: %s. "+
					"These catalogs may need to be reconfigured or reconnected.",
				strings.Join(unavailableCatalogs, ", "),
			),
		)
	}

	return messages, nil
}

func (e *TrinoScrapper) Close() error {
	return e.executor.Close()
}

func (e *TrinoScrapper) Executor() *dwhexectrino.TrinoExecutor {
	return e.executor
}

func (e *TrinoScrapper) fqn(row scrapper.HasTableFqn, dollarTable ...string) interface{} {
	fqn := row.TableFqn()
	table := fqn.ObjectName
	if len(dollarTable) > 0 {
		table = fmt.Sprintf("%s$%s", table, dollarTable[0])
	}

	return fmt.Sprintf("%q.%q.%q", fqn.DatabaseName, fqn.SchemaName, table)
}

// isCatalogUnavailableError checks if the error indicates a catalog is unavailable or inaccessible
func isCatalogUnavailableError(err error) bool {
	if err == nil {
		return false
	}
	errMsg := strings.ToLower(err.Error())
	// Common patterns for unavailable Trino catalogs:
	// - "catalog 'xyz' not found"
	// - "catalog 'xyz' does not exist"
	// - "line 1:15: catalog 'xyz' not registered"
	// - "EXTERNAL: Error listing tables for catalog xyz: The connection attempt failed."
	// - Connection failures to the catalog's data source
	// - "schema does not exist" when probing a catalog
	return strings.Contains(errMsg, "catalog") &&
		(strings.Contains(errMsg, "not found") ||
			strings.Contains(errMsg, "does not exist") ||
			strings.Contains(errMsg, "not registered") ||
			strings.Contains(errMsg, "connection") ||
			strings.Contains(errMsg, "unreachable") ||
			strings.Contains(errMsg, "unavailable") ||
			strings.Contains(errMsg, "error listing"))
}
