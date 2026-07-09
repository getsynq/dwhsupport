package mssql

import (
	"context"

	"github.com/getsynq/dwhsupport/scrapper"
)

// EstimateQuery is unsupported for now. SQL Server's estimated plan comes from
// `SET SHOWPLAN_XML ON`, a session-mode toggle that returns the plan as an XML
// document rather than a queryable estimate, and the EstimateRows/
// EstimatedDataSize attributes depend on current statistics. Returning
// ErrUnsupported until a validation spike confirms a reliable parse.
func (e *MSSQLScrapper) EstimateQuery(ctx context.Context, sql string) (*scrapper.QueryEstimate, error) {
	return nil, scrapper.ErrUnsupported
}
