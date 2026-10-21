package bigquery

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoutineFromValues(t *testing.T) {
	procedure := routineFromValues(map[string]bigquery.Value{
		"database":     "my-project",
		"schema":       "my_dataset",
		"name":         "purge_old_events",
		"routine_type": "PROCEDURE",
		"language":     "SQL",
		"arguments":    "days_back INT64",
		"return_type":  nil,
		"definition":   "CREATE PROCEDURE `my-project.my_dataset.purge_old_events`(days_back INT64) BEGIN SELECT 1; END",
		"description":  `"Deletes \"old\" events"`,
	})
	assert.Equal(t, "my-project", procedure.Database)
	assert.Equal(t, "my_dataset", procedure.Schema)
	assert.Equal(t, "purge_old_events", procedure.Name)
	assert.Equal(t, scrapper.RoutineKindProcedure, procedure.Kind)
	assert.Equal(t, "PROCEDURE", procedure.RoutineType)
	assert.Equal(t, "SQL", *procedure.Language)
	assert.Equal(t, "days_back INT64", *procedure.Arguments)
	assert.Nil(t, procedure.ReturnType)
	require.NotNil(t, procedure.Definition)
	require.NotNil(t, procedure.Description)
	assert.Equal(t, `Deletes "old" events`, *procedure.Description)

	tableFunction := routineFromValues(map[string]bigquery.Value{
		"database":     "my-project",
		"schema":       "my_dataset",
		"name":         "orders_for_product",
		"routine_type": "TABLE FUNCTION",
		"language":     "SQL",
		"arguments":    nil,
		"description":  nil,
	})
	assert.Equal(t, scrapper.RoutineKindFunction, tableFunction.Kind)
	assert.Equal(t, "TABLE FUNCTION", tableFunction.RoutineType)
	assert.Nil(t, tableFunction.Arguments)
	assert.Nil(t, tableFunction.Definition, "a routine whose DDL the role may not read has none")
	assert.Nil(t, tableFunction.Description)
}

func TestUnquoteOptionValue(t *testing.T) {
	assert.Equal(t, "plain", *unquoteOptionValue(`"plain"`))
	assert.Equal(t, "not quoted", *unquoteOptionValue("not quoted"))
	assert.Nil(t, unquoteOptionValue(`""`))
}
