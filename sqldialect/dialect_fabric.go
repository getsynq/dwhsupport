package sqldialect

import (
	"fmt"

	"github.com/pkg/errors"
)

// FabricDialect targets Microsoft Fabric Warehouse (the T-SQL surface exposed by
// the workspace SQL analytics endpoint). Fabric Warehouse runs a Synapse-derived
// engine ("Azure SQL Data Warehouse 12.0.2000.8") whose T-SQL is a subset of SQL
// Server, so it reuses the MSSQL dialect for almost everything and only overrides
// where Fabric genuinely diverges.
//
// Known divergence handled here:
//   - No NVARCHAR / NCHAR. Fabric Warehouse rejects the national-character types
//     entirely (validated against the live engine — see the dwhtesting Fabric
//     seed). So string casts must target VARCHAR(MAX), not NVARCHAR(MAX).
//
// Everything else the MSSQL dialect emits (LEN, DATEADD/DATEDIFF truncation,
// CONCAT_WS, GETUTCDATE(), CAST AS FLOAT, TOP row-capping, PERCENTILE_CONT
// being window-only so Median→NULL, bracket quoting) is valid Fabric T-SQL.
type FabricDialect struct {
	*MSSQLDialect
}

var _ Dialect = (*FabricDialect)(nil)

func NewFabricDialect() *FabricDialect {
	return &FabricDialect{MSSQLDialect: NewMSSQLDialect()}
}

// ToString casts to VARCHAR(MAX). Fabric Warehouse has no NVARCHAR, so the
// MSSQL dialect's CAST(... AS NVARCHAR(MAX)) would be rejected at parse time.
func (d *FabricDialect) ToString(expr Expr) Expr {
	return WrapSql("CAST(%s AS VARCHAR(MAX))", expr)
}

// SupportsCrossDatabaseQueries is true for Fabric: the workspace SQL endpoint
// exposes every warehouse/lakehouse in the workspace as a database and supports
// three-part-name queries across them (verified live). This is what makes the
// integration workspace-centric rather than bound to a single database.
func (d *FabricDialect) SupportsCrossDatabaseQueries() bool { return true }

// ResolveFqn emits a three-part [database].[schema].[table] name when a database
// (projectId) is present, so monitors/metrics can target tables in any workspace
// database from a single connection. Falls back to [schema].[table] otherwise.
func (d *FabricDialect) ResolveFqn(fqn *TableFqnExpr) (string, error) {
	if fqn == nil {
		return "", errors.New("fqn is nil")
	}
	if fqn.projectId != "" {
		return fmt.Sprintf("[%s].[%s].[%s]", fqn.projectId, fqn.datasetId, fqn.tableId), nil
	}
	return fmt.Sprintf("[%s].[%s]", fqn.datasetId, fqn.tableId), nil
}
