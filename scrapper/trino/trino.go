package trino

import (
	"context"
	"fmt"

	dwhexectrino "github.com/getsynq/dwhsupport/exec/trino"
	"github.com/getsynq/dwhsupport/lazy"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/samber/lo"
)

type TrinoScrapperConf struct {
	*dwhexectrino.TrinoConf
	Catalogs           []string
	UseShowCreateView  bool
	UseShowCreateTable bool
}

var _ scrapper.Scrapper = &TrinoScrapper{}

type TrinoScrapper struct {
	conf                 *TrinoScrapperConf
	executor             *dwhexectrino.TrinoExecutor
	allAvailableCatalogs lazy.Lazy[[]*TrinoCatalog]
}

type TrinoCatalog struct {
	CatalogName   string `db:"catalog_name" json:"catalog_name"`
	ConnectorId   string `db:"connector_id" json:"connector_id"`
	ConnectorName string `db:"connector_name" json:"connector_name"`
	IsAccepted    bool   `db:"-" json:"is_accepted"`
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
		rows, err := executor.GetDb().QueryxContext(ctx, fmt.Sprintf("SELECT * FROM system.metadata.catalogs"))
		if err != nil {
			return nil, err
		}
		defer rows.Close()

		var allCatalogs []*TrinoCatalog
		for rows.Next() {
			cat := &TrinoCatalog{}
			if err := rows.StructScan(cat); err != nil {
				return nil, err
			}

			if lo.Contains(conf.Catalogs, cat.CatalogName) {
				cat.IsAccepted = true
			}

			allCatalogs = append(allCatalogs, cat)
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
	var missingCatalogs []string
	for _, catalog := range e.conf.Catalogs {
		found := false
		for _, availableCatalog := range availableCatalogs {
			if catalog == availableCatalog.CatalogName {
				found = true
				break
			}
		}
		if !found {
			missingCatalogs = append(missingCatalogs, catalog)
		}
	}

	var messages []string

	if len(missingCatalogs) > 0 {
		if len(missingCatalogs) > 0 {
			messages = append(messages, fmt.Sprintf("The following catalogs are not available: %s", missingCatalogs))
		}
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
