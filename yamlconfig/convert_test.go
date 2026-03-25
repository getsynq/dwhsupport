package yamlconfig

import (
	"os"
	"testing"

	agentdwhv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/agent/dwh/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestToProtoConnections_AllTypes(t *testing.T) {
	data, err := os.ReadFile("testdata/full_example.yaml")
	require.NoError(t, err)

	conns, err := ParseConnections(data, ParseOptions{})
	require.NoError(t, err)

	protos, err := ToProtoConnections(conns)
	require.NoError(t, err)
	assert.Len(t, protos, 12)

	// Verify Postgres
	pg := protos["pg-local"]
	require.NotNil(t, pg)
	assert.Equal(t, "PG Local", pg.GetName())
	pgConf := pg.GetPostgres()
	require.NotNil(t, pgConf)
	assert.Equal(t, "localhost", pgConf.GetHost())
	assert.Equal(t, int32(54320), pgConf.GetPort())
	assert.True(t, pgConf.GetAllowInsecure())

	// Verify MySQL with params
	mysql := protos["mysql-prod"]
	require.NotNil(t, mysql)
	assert.Equal(t, int32(4), mysql.GetParallelism())
	mysqlConf := mysql.GetMysql()
	require.NotNil(t, mysqlConf)
	assert.Equal(t, "utf8mb4", mysqlConf.GetParams()["charset"])

	// Verify BigQuery disabled
	bq := protos["bq-project"]
	require.NotNil(t, bq)
	assert.True(t, bq.GetDisabled())
	bqConf := bq.GetBigquery()
	require.NotNil(t, bqConf)
	assert.Equal(t, "my-gcp-project", bqConf.GetProjectId())

	// Verify Snowflake
	sf := protos["sf-main"]
	require.NotNil(t, sf)
	sfConf := sf.GetSnowflake()
	require.NotNil(t, sfConf)
	assert.Equal(t, "SYNQ_USER", sfConf.GetUsername())
	assert.Equal(t, []string{"ANALYTICS", "STAGING"}, sfConf.GetDatabases())
	assert.True(t, sfConf.GetUseGetDdl())
	assert.Equal(t, "SNOWFLAKE", sfConf.GetAccountUsageDb())

	// Verify Trino
	trino := protos["trino-galaxy"]
	require.NotNil(t, trino)
	trinoConf := trino.GetTrino()
	require.NotNil(t, trinoConf)
	assert.Equal(t, []string{"tpch", "iceberg_gcs"}, trinoConf.GetCatalogs())
	assert.True(t, trinoConf.GetFetchTableComments())

	// Verify Databricks with OAuth
	dbr := protos["databricks-prod"]
	require.NotNil(t, dbr)
	dbrConf := dbr.GetDatabricks()
	require.NotNil(t, dbrConf)
	assert.True(t, dbrConf.GetRefreshTableMetrics())
	assert.True(t, dbrConf.GetFetchTableTags())
	assert.Equal(t, "abc123def456", dbrConf.GetWarehouse())

	// Verify MSSQL
	mssql := protos["mssql-azure"]
	require.NotNil(t, mssql)
	mssqlConf := mssql.GetMssql()
	require.NotNil(t, mssqlConf)
	assert.Equal(t, int32(1433), mssqlConf.GetPort())
	assert.True(t, mssqlConf.GetTrustCert())

	// Verify Oracle
	ora := protos["oracle-prod"]
	require.NotNil(t, ora)
	oraConf := ora.GetOracle()
	require.NotNil(t, oraConf)
	assert.Equal(t, "ORCL", oraConf.GetServiceName())
	assert.True(t, oraConf.GetSsl())
	assert.True(t, oraConf.GetUseDiagnosticsPack())

	// Verify DuckDB
	duck := protos["duckdb-local"]
	require.NotNil(t, duck)
	duckConf := duck.GetDuckdb()
	require.NotNil(t, duckConf)
	assert.Equal(t, ":memory:", duckConf.GetDatabase())

	// Verify MotherDuck
	md := protos["motherduck"]
	require.NotNil(t, md)
	mdConf := md.GetDuckdb()
	require.NotNil(t, mdConf)
	assert.Equal(t, "my-org", mdConf.GetMotherduckAccount())

	// Verify Redshift
	rs := protos["redshift-prod"]
	require.NotNil(t, rs)
	rsConf := rs.GetRedshift()
	require.NotNil(t, rsConf)
	assert.Equal(t, int32(5439), rsConf.GetPort())
	assert.True(t, rsConf.GetFreshnessFromQueryLogs())
}

func TestToProtoConnection_DefaultName(t *testing.T) {
	conn := &Connection{
		Postgres: &PostgresConf{
			Host: "localhost", Port: 5432, Username: "u", Password: "p", Database: "db",
		},
	}

	proto, err := ToProtoConnection("my-conn-id", conn)
	require.NoError(t, err)
	assert.Equal(t, "my-conn-id", proto.GetName())
}

func TestToProtoConnection_ExplicitName(t *testing.T) {
	conn := &Connection{
		Name: "My Postgres",
		Postgres: &PostgresConf{
			Host: "localhost", Port: 5432, Username: "u", Password: "p", Database: "db",
		},
	}

	proto, err := ToProtoConnection("conn-id", conn)
	require.NoError(t, err)
	assert.Equal(t, "My Postgres", proto.GetName())
}

func TestToProtoConnection_NilConn(t *testing.T) {
	_, err := ToProtoConnection("id", nil)
	require.Error(t, err)
}

func TestToProtoConnection_NoDialect(t *testing.T) {
	_, err := ToProtoConnection("id", &Connection{Name: "empty"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no database type")
}

func TestToProtoConnection_TrinoOptionalFields(t *testing.T) {
	conn := &Connection{
		Trino: &TrinoConf{
			Host:         "trino.example.com",
			Port:         8443,
			UsePlaintext: true,
		},
	}

	proto, err := ToProtoConnection("trino", conn)
	require.NoError(t, err)
	trinoConf := proto.GetTrino()
	require.NotNil(t, trinoConf)
	assert.Equal(t, int32(8443), trinoConf.GetPort())
	assert.True(t, trinoConf.HasPort())
	assert.True(t, trinoConf.GetUsePlaintext())
	assert.True(t, trinoConf.HasUsePlaintext())
}

func TestToProtoConnection_TrinoZeroPort(t *testing.T) {
	conn := &Connection{
		Trino: &TrinoConf{
			Host: "trino.example.com",
		},
	}

	proto, err := ToProtoConnection("trino", conn)
	require.NoError(t, err)
	trinoConf := proto.GetTrino()
	require.NotNil(t, trinoConf)
	assert.False(t, trinoConf.HasPort())
}

func TestToProtoConnection_DatabricksOptionalFields(t *testing.T) {
	conn := &Connection{
		Databricks: &DatabricksConf{
			WorkspaceUrl: "https://example.com",
			AuthToken:    "tok123",
			Warehouse:    "wh-id",
		},
	}

	proto, err := ToProtoConnection("dbr", conn)
	require.NoError(t, err)
	dbrConf := proto.GetDatabricks()
	require.NotNil(t, dbrConf)
	assert.True(t, dbrConf.HasAuthToken())
	assert.Equal(t, "tok123", dbrConf.GetAuthToken())
	assert.True(t, dbrConf.HasWarehouse())
	assert.False(t, dbrConf.HasAuthClient())
}

func TestToProtoConnection_SnowflakeOptionalFields(t *testing.T) {
	conn := &Connection{
		Snowflake: &SnowflakeConf{
			Account:              "acc",
			Warehouse:            "wh",
			Role:                 "role",
			Username:             "user",
			PrivateKeyPassphrase: "passphrase",
			AccountUsageDb:       "CUSTOM_DB",
			AuthType:             "externalbrowser",
		},
	}

	proto, err := ToProtoConnection("sf", conn)
	require.NoError(t, err)
	sfConf := proto.GetSnowflake()
	require.NotNil(t, sfConf)
	assert.True(t, sfConf.HasPrivateKeyPassphrase())
	assert.Equal(t, "passphrase", sfConf.GetPrivateKeyPassphrase())
	assert.True(t, sfConf.HasAccountUsageDb())
	assert.Equal(t, "CUSTOM_DB", sfConf.GetAccountUsageDb())
	assert.True(t, sfConf.HasAuthType())
	assert.Equal(t, "externalbrowser", sfConf.GetAuthType())
}

func TestRoundTrip_AllTypes(t *testing.T) {
	data, err := os.ReadFile("testdata/full_example.yaml")
	require.NoError(t, err)

	// Parse YAML → structs
	conns, err := ParseConnections(data, ParseOptions{})
	require.NoError(t, err)

	// Convert to proto
	protos, err := ToProtoConnections(conns)
	require.NoError(t, err)

	// Convert back to structs
	roundTripped := FromProtoConnections(protos)

	// Verify each connection round-trips
	for id, original := range conns {
		rt := roundTripped[id]
		require.NotNil(t, rt, "missing connection %s after round-trip", id)
		assert.Equal(t, original.DialectType(), rt.DialectType(), "dialect mismatch for %s", id)

		// Name: proto uses connection ID as default name if empty,
		// so round-tripped name may differ from original empty name
		if original.Name != "" {
			assert.Equal(t, original.Name, rt.Name, "name mismatch for %s", id)
		}
	}
}

func TestFromProtoConnection_Nil(t *testing.T) {
	assert.Nil(t, FromProtoConnection(nil))
}

func TestFromProtoConnection_Postgres(t *testing.T) {
	proto := &agentdwhv1.Connection{
		Name:        "test-pg",
		Parallelism: 4,
		Config: &agentdwhv1.Connection_Postgres{
			Postgres: &agentdwhv1.PostgresConf{
				Host:          "localhost",
				Port:          5432,
				Database:      "mydb",
				Username:      "admin",
				Password:      "secret",
				AllowInsecure: true,
			},
		},
	}

	conn := FromProtoConnection(proto)
	require.NotNil(t, conn)
	assert.Equal(t, "test-pg", conn.Name)
	assert.Equal(t, 4, conn.Parallelism)
	assert.Equal(t, "postgres", conn.DialectType())
	require.NotNil(t, conn.Postgres)
	assert.Equal(t, "localhost", conn.Postgres.Host)
	assert.Equal(t, 5432, conn.Postgres.Port)
	assert.True(t, conn.Postgres.AllowInsecure)
}
