// Package fabric implements a scrapper for Microsoft Fabric Warehouses and
// Lakehouse SQL analytics endpoints.
//
// Fabric speaks T-SQL over TDS, so it shares the MSSQL executor and (mostly) the
// MSSQL dialect. It is a first-class scrapper rather than a thin MSSQL alias
// because Fabric Warehouse's metadata surface diverges meaningfully from SQL
// Server — no page-based storage stats, informational-only NOT ENFORCED
// constraints (including foreign keys), no extended properties, no NVARCHAR — so
// every metadata query here is written against the Fabric surface directly.
package fabric

import (
	"context"

	dwhexecfabric "github.com/getsynq/dwhsupport/exec/fabric"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/sqldialect"
)

// FabricScrapperConf configures a Fabric scrapper. It embeds the executor conf
// (the simplified Fabric connection surface) per the repo convention.
type FabricScrapperConf struct {
	dwhexecfabric.FabricConf
}

var _ scrapper.Scrapper = &FabricScrapper{}

type FabricScrapper struct {
	conf     *FabricScrapperConf
	executor *dwhexecfabric.FabricExecutor
}

func NewFabricScrapper(ctx context.Context, conf *FabricScrapperConf) (*FabricScrapper, error) {
	executor, err := dwhexecfabric.NewFabricExecutor(ctx, &conf.FabricConf)
	if err != nil {
		return nil, err
	}

	return &FabricScrapper{
		conf:     conf,
		executor: executor,
	}, nil
}

func (e *FabricScrapper) Executor() *dwhexecfabric.FabricExecutor {
	return e.executor
}

func (e *FabricScrapper) IsPermissionError(err error) bool {
	return dwhexecfabric.IsPermissionError(err)
}

func (e *FabricScrapper) Capabilities() scrapper.Capabilities { return scrapper.Capabilities{} }

func (e *FabricScrapper) DialectType() string {
	return "fabric"
}

func (e *FabricScrapper) SqlDialect() sqldialect.Dialect {
	return sqldialect.NewFabricDialect()
}

func (e *FabricScrapper) ValidateConfiguration(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (e *FabricScrapper) Close() error {
	return e.executor.Close()
}
