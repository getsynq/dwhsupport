package fabric

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

// EstimateQuery is unsupported for Fabric Warehouse. Like SQL Server, the
// estimated plan is only available via the SHOWPLAN_XML session toggle (an XML
// document, not a queryable estimate) and depends on current statistics.
// Capabilities().EstimateQuery is left zero-valued so callers skip it.
func (e *FabricScrapper) EstimateQuery(ctx context.Context, sql string) (*scrapper.QueryEstimate, error) {
	return nil, scrapper.ErrUnsupported
}
