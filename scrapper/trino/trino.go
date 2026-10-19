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
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/trinodb/trino-go-client/trino"
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

	allAvailableCatalogs := lazy.New(func() ([]*TrinoCatalog, error) {
		db := executor.GetDb()
		allCatalogs, err := stdsql.QueryMany(ctx, db, "SELECT catalog_name, connector_id, connector_name FROM system.metadata.catalogs",
			dwhexec.WithPostProcessors(func(row *TrinoCatalog) (*TrinoCatalog, error) {
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
	return dwhexectrino.IsPermissionError(err)
}

func (e *TrinoScrapper) Capabilities() scrapper.Capabilities { return scrapper.Capabilities{} }

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

		// Probe the catalog to check if it's accessible with a 15 second timeout
		// We use a simple query against information_schema which will fail if the catalog is unavailable
		probeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		probeQuery := fmt.Sprintf("SELECT 1 FROM %s.information_schema.schemata LIMIT 1", catalog)
		probeRows, probeErr := e.executor.QueryRows(probeCtx, probeQuery)
		if probeRows != nil {
			probeRows.Close()
		}
		// The driver wraps a timeout in its own error, so ask the probe's context
		// whether it ran out rather than comparing the error. A catalog whose
		// backend is gone can take longer than the probe to say so.
		timedOut := errors.Is(probeCtx.Err(), context.DeadlineExceeded)
		cancel()

		if probeErr != nil {
			if isCatalogUnavailableError(probeErr) || timedOut {
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

// acceptedCatalogs returns the subset of all available catalogs that are accepted
// by conf.Catalogs and, if a scope filter is present in ctx, by that filter's
// database rules. This is the single place for catalog-level filtering; all
// scrapper query methods should call this instead of allAvailableCatalogs.Get().
func (e *TrinoScrapper) acceptedCatalogs(ctx context.Context) ([]*TrinoCatalog, error) {
	allCatalogs, err := e.allAvailableCatalogs.Get()
	if err != nil {
		return nil, err
	}
	scopeFilter := scope.GetScope(ctx)
	result := make([]*TrinoCatalog, 0, len(allCatalogs))
	for _, catalog := range allCatalogs {
		if !catalog.IsAccepted {
			continue
		}
		if scopeFilter != nil && !scopeFilter.IsDatabaseAccepted(catalog.CatalogName) {
			continue
		}
		result = append(result, catalog)
	}
	return result, nil
}

func (e *TrinoScrapper) Close() error {
	return e.executor.Close()
}

func (e *TrinoScrapper) Executor() *dwhexectrino.TrinoExecutor {
	return e.executor
}

func (e *TrinoScrapper) fqn(row scrapper.HasTableFqn, dollarTable ...string) any {
	fqn := row.TableFqn()
	table := fqn.ObjectName
	if len(dollarTable) > 0 {
		table = fmt.Sprintf("%s$%s", table, dollarTable[0])
	}

	return fmt.Sprintf("%q.%q.%q", fqn.DatabaseName, fqn.SchemaName, table)
}

// unavailableCatalogErrorNames are the Trino error codes (StandardErrorCode)
// that say the catalog itself is gone: dropped, never registered, or failed to
// load on the coordinator.
var unavailableCatalogErrorNames = map[string]bool{
	"CATALOG_NOT_FOUND":     true,
	"CATALOG_UNAVAILABLE":   true,
	"CATALOG_NOT_AVAILABLE": true,
}

// isCatalogUnavailableError reports an error that says the catalog is gone, or
// that its connector cannot reach the database behind it, which is when a
// caller not accepting incomplete results skips the catalog.
//
// It decides by the error Trino reported, never by the message text alone. A
// catalog whose tables cannot be read fails with "Error listing table columns
// for catalog iceberg: Failed to open input stream …", which names the catalog
// and says listing failed, and skipping that catalog would publish its tables
// without columns. A JDBC catalog whose database is down answers EXTERNAL "The
// connection attempt failed.", with or without the "Error listing tables for
// catalog" prefix, and that one is unavailable.
func isCatalogUnavailableError(err error) bool {
	var trinoErr *trino.ErrTrino
	if !errors.As(err, &trinoErr) {
		return false
	}
	if unavailableCatalogErrorNames[trinoErr.ErrorName] {
		return true
	}
	return trinoErr.ErrorType == "EXTERNAL" &&
		strings.Contains(strings.ToLower(trinoErr.Message), "the connection attempt failed")
}

// isTableNotFoundError checks if the error indicates a table does not exist.
// Some Trino catalogs (e.g. tpch, tpcds) do not expose information_schema.table_constraints.
func isTableNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	errMsg := strings.ToLower(err.Error())
	return strings.Contains(errMsg, "does not exist") && strings.Contains(errMsg, "table")
}
