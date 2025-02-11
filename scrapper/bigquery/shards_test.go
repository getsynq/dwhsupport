package bigquery

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestShardedTableName(t *testing.T) {
	tests := []struct {
		name          string
		input         string
		expectedName  string
		expectedValid bool
	}{
		{
			name:          "valid sharded table",
			input:         "events_20240315",
			expectedName:  "events_",
			expectedValid: true,
		},
		{
			name:          "valid sharded table without underscore",
			input:         "events20240315",
			expectedName:  "events",
			expectedValid: true,
		},
		{
			name:          "invalid - no date suffix",
			input:         "events",
			expectedName:  "",
			expectedValid: false,
		},
		{
			name:          "invalid - wrong date format",
			input:         "events_2024",
			expectedName:  "",
			expectedValid: false,
		},
		{
			name:          "invalid - no date suffix",
			input:         "random_46428353",
			expectedName:  "",
			expectedValid: false,
		},
		{
			name:          "invalid - more digits in valid date suffix",
			input:         "sqlmesh_example__incremental_model__520240315",
			expectedName:  "",
			expectedValid: false,
		},
		{
			name:          "invalid - date only",
			input:         "20240315",
			expectedName:  "",
			expectedValid: false,
		},
		{
			name:          "invalid - date outside of valid upper range",
			input:         "events_20900101",
			expectedName:  "",
			expectedValid: false,
		},
		{
			name:          "invalid - date outside of valid lower range",
			input:         "events_19000101",
			expectedName:  "",
			expectedValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			name, ok := shardedTableName(
				tt.input,
				time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
				time.Date(2054, 1, 3, 0, 0, 0, 0, time.UTC),
			)
			assert.Equal(t, tt.expectedName, name)
			assert.Equal(t, tt.expectedValid, ok)
		})
	}
}
