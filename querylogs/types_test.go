package querylogs

import (
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/stretchr/testify/require"
)

func TestNativeLineage_GetInputTables(t *testing.T) {
	tests := []struct {
		name     string
		lineage  *NativeLineage
		expected []scrapper.DwhFqn
	}{
		{
			name:     "nil lineage returns empty slice",
			lineage:  nil,
			expected: []scrapper.DwhFqn{},
		},
		{
			name:     "nil InputTables returns empty slice",
			lineage:  &NativeLineage{InputTables: nil},
			expected: []scrapper.DwhFqn{},
		},
		{
			name:     "empty InputTables returns empty slice",
			lineage:  &NativeLineage{InputTables: []scrapper.DwhFqn{}},
			expected: []scrapper.DwhFqn{},
		},
		{
			name: "populated InputTables returns tables",
			lineage: &NativeLineage{
				InputTables: []scrapper.DwhFqn{
					{DatabaseName: "db1", SchemaName: "schema1", ObjectName: "table1"},
					{DatabaseName: "db2", SchemaName: "schema2", ObjectName: "table2"},
				},
			},
			expected: []scrapper.DwhFqn{
				{DatabaseName: "db1", SchemaName: "schema1", ObjectName: "table1"},
				{DatabaseName: "db2", SchemaName: "schema2", ObjectName: "table2"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.lineage.GetInputTables()
			require.NotNil(t, result, "GetInputTables should never return nil")
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestNativeLineage_GetOutputTables(t *testing.T) {
	tests := []struct {
		name     string
		lineage  *NativeLineage
		expected []scrapper.DwhFqn
	}{
		{
			name:     "nil lineage returns empty slice",
			lineage:  nil,
			expected: []scrapper.DwhFqn{},
		},
		{
			name:     "nil OutputTables returns empty slice",
			lineage:  &NativeLineage{OutputTables: nil},
			expected: []scrapper.DwhFqn{},
		},
		{
			name:     "empty OutputTables returns empty slice",
			lineage:  &NativeLineage{OutputTables: []scrapper.DwhFqn{}},
			expected: []scrapper.DwhFqn{},
		},
		{
			name: "populated OutputTables returns tables",
			lineage: &NativeLineage{
				OutputTables: []scrapper.DwhFqn{
					{DatabaseName: "db1", SchemaName: "schema1", ObjectName: "output1"},
				},
			},
			expected: []scrapper.DwhFqn{
				{DatabaseName: "db1", SchemaName: "schema1", ObjectName: "output1"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.lineage.GetOutputTables()
			require.NotNil(t, result, "GetOutputTables should never return nil")
			require.Equal(t, tt.expected, result)
		})
	}
}
