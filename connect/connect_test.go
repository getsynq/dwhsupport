package connect

import (
	"testing"

	agentdwhv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/agent/dwh/v1"
	"github.com/stretchr/testify/assert"
)

func TestEffectiveDatabaseName(t *testing.T) {
	tests := []struct {
		name     string
		conf     *agentdwhv1.Connection
		expected string
	}{
		{
			name:     "nil connection returns empty",
			conf:     nil,
			expected: "",
		},
		{
			name:     "nil config returns empty",
			conf:     &agentdwhv1.Connection{},
			expected: "",
		},
		{
			name: "clickhouse prefers database over host",
			conf: &agentdwhv1.Connection{
				Config: &agentdwhv1.Connection_Clickhouse{
					Clickhouse: &agentdwhv1.ClickhouseConf{
						Host:     "MY_HOST.region-1.provider.cloud",
						Database: "my_database",
					},
				},
			},
			expected: "my_database",
		},
		{
			name: "clickhouse falls back to host when database is empty",
			conf: &agentdwhv1.Connection{
				Config: &agentdwhv1.Connection_Clickhouse{
					Clickhouse: &agentdwhv1.ClickhouseConf{
						Host: "MY_HOST.region-1.provider.cloud",
					},
				},
			},
			expected: "MY_HOST.region-1.provider.cloud",
		},
		{
			name: "postgres returns database",
			conf: &agentdwhv1.Connection{
				Config: &agentdwhv1.Connection_Postgres{
					Postgres: &agentdwhv1.PostgresConf{
						Host:     "MY_HOST",
						Database: "MY_DB",
					},
				},
			},
			expected: "MY_DB",
		},
		{
			name: "redshift returns database",
			conf: &agentdwhv1.Connection{
				Config: &agentdwhv1.Connection_Redshift{
					Redshift: &agentdwhv1.RedshiftConf{
						Host:     "MY_HOST",
						Database: "MY_DB",
					},
				},
			},
			expected: "MY_DB",
		},
		{
			name: "bigquery returns project id",
			conf: &agentdwhv1.Connection{
				Config: &agentdwhv1.Connection_Bigquery{
					Bigquery: &agentdwhv1.BigQueryConf{
						ProjectId: "MY_PROJECT",
					},
				},
			},
			expected: "MY_PROJECT",
		},
		{
			name: "mysql returns host",
			conf: &agentdwhv1.Connection{
				Config: &agentdwhv1.Connection_Mysql{
					Mysql: &agentdwhv1.MySQLConf{
						Host: "MY_HOST",
					},
				},
			},
			expected: "MY_HOST",
		},
		{
			name: "snowflake with single database returns it",
			conf: &agentdwhv1.Connection{
				Config: &agentdwhv1.Connection_Snowflake{
					Snowflake: &agentdwhv1.SnowflakeConf{
						Databases: []string{"MY_DB"},
					},
				},
			},
			expected: "MY_DB",
		},
		{
			name: "snowflake with duplicate databases returns it",
			conf: &agentdwhv1.Connection{
				Config: &agentdwhv1.Connection_Snowflake{
					Snowflake: &agentdwhv1.SnowflakeConf{
						Databases: []string{"MY_DB", "MY_DB"},
					},
				},
			},
			expected: "MY_DB",
		},
		{
			name: "snowflake with multiple databases returns empty",
			conf: &agentdwhv1.Connection{
				Config: &agentdwhv1.Connection_Snowflake{
					Snowflake: &agentdwhv1.SnowflakeConf{
						Databases: []string{"MY_DB", "MY_OTHER_DB"},
					},
				},
			},
			expected: "",
		},
		{
			name: "snowflake with no databases returns empty",
			conf: &agentdwhv1.Connection{
				Config: &agentdwhv1.Connection_Snowflake{
					Snowflake: &agentdwhv1.SnowflakeConf{},
				},
			},
			expected: "",
		},
		{
			name: "databricks returns empty",
			conf: &agentdwhv1.Connection{
				Config: &agentdwhv1.Connection_Databricks{
					Databricks: &agentdwhv1.DatabricksConf{
						WorkspaceUrl: "MY_WORKSPACE_URL",
					},
				},
			},
			expected: "",
		},
		{
			name: "trino returns empty",
			conf: &agentdwhv1.Connection{
				Config: &agentdwhv1.Connection_Trino{
					Trino: &agentdwhv1.TrinoConf{
						Host: "MY_HOST",
					},
				},
			},
			expected: "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, EffectiveDatabaseName(tc.conf))
		})
	}
}
