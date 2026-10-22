package oracle

import (
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

func TestFunctionBasedIndexRow(t *testing.T) {
	row := &oracleFunctionBasedIndexRow{
		Schema:           "SYNQ_A",
		Table:            "PRODUCTS",
		ConstraintName:   "IDX_PRODUCTS_NAME_UPPER",
		ColumnName:       "SYS_NC00013$",
		ColumnExpression: lo.ToPtr(`UPPER("NAME")`),
		ConstraintType:   scrapper.ConstraintTypeIndex,
		ColumnPosition:   1,
		IsEnforced:       1,
		IndexType:        "FUNCTION-BASED NORMAL",
	}
	require.Equal(t, &scrapper.TableConstraintRow{
		Instance:       "host",
		Database:       "FREEPDB1",
		Schema:         "SYNQ_A",
		Table:          "PRODUCTS",
		ConstraintName: "IDX_PRODUCTS_NAME_UPPER",
		ColumnName:     `UPPER("NAME")`,
		ConstraintType: scrapper.ConstraintTypeIndex,
		ColumnPosition: 1,
		IsEnforced:     lo.ToPtr(true),
		IndexType:      "FUNCTION-BASED NORMAL",
	}, functionBasedIndexRow("host", "FREEPDB1", row))

	// a plain column of the same index has no expression and keeps its name
	row.ColumnName, row.ColumnExpression, row.ColumnPosition = "CATEGORY", nil, 2
	require.Equal(t, "CATEGORY", functionBasedIndexRow("host", "FREEPDB1", row).ColumnName)

	// an index Oracle disabled because its function changed is not enforced
	row.IsEnforced = 0
	require.Equal(t, lo.ToPtr(false), functionBasedIndexRow("host", "FREEPDB1", row).IsEnforced)
}
