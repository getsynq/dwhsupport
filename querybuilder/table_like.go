package querybuilder

import (
	. "github.com/getsynq/dwhsupport/sqldialect"
)

type TableLike interface {
	Fqn() *TableFqnExpr

	// TextCol(name string) *TextColExpr
	// NumericCol(name string) *NumericColExpr
	// TimeCol(name string) *TimeColExpr
}

type MockTable struct {
	projectId string
	datasetId string
	tableId   string
}

func NewMockTable(projectId, datasetId, tableId string) *MockTable {
	return &MockTable{
		projectId: projectId,
		datasetId: datasetId,
		tableId:   tableId,
	}
}

func (t *MockTable) Fqn() *TableFqnExpr {
	return TableFqn(t.projectId, t.datasetId, t.tableId)
}

func (t *MockTable) TextCol(name string) *TextColExpr {
	return TextCol(name)
}

func (t *MockTable) NumericCol(name string) *NumericColExpr {
	return NumericCol(name)
}

func (t *MockTable) TimeCol(name string) *TimeColExpr {
	return TimeCol(name)
}
