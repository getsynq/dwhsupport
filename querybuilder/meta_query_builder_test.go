package querybuilder

import (
	"testing"

	. "github.com/getsynq/dwhsupport/sqldialect"
	"github.com/gkampitakis/go-snaps/snaps"
	"github.com/stretchr/testify/require"
)

func TestMetaQueryBuilderSnapshot(t *testing.T) {
	testCases := []struct {
		name      string
		table     TableExpr
		shouldErr bool
	}{
		{
			name:      "StandardTable",
			table:     TableFqn("my_project", "my_schema", "my_table"),
			shouldErr: false,
		},
	}

	dialects := []struct {
		name    string
		dialect Dialect
	}{
		{"clickhouse", NewClickHouseDialect()},
		// {"postgres", NewPostgresDialect()},
		// {"trino", NewTrinoDialect()},
		// {"bigquery", NewBigQueryDialect()},
		// {"redshift", NewRedshiftDialect()},
		// {"snowflake", NewSnowflakeDialect()},
		// {"duckdb", NewDuckDBDialect()},
		// {"mysql", NewMySQLDialect()},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			builder := NewMetaQueryBuilder(tc.table)

			for _, d := range dialects {
				t.Run(d.name, func(t *testing.T) {
					query, err := builder.ToSql(d.dialect)

					if tc.shouldErr {
						require.Error(t, err)
						return
					}

					require.NoError(t, err)
					snaps.WithConfig(
						snaps.Filename("meta_query_builder"),
					).MatchSnapshot(t, query)
				})
			}
		})
	}
}
