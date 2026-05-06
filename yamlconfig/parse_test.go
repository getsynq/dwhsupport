
package yamlconfig

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseConnections_FullExample(t *testing.T) {
	data, err := os.ReadFile("testdata/full_example.yaml")
	require.NoError(t, err)

	// Parse without env expansion — env vars remain as literal "${VAR}"
	conns, err := ParseConnections(data, ParseOptions{})
	require.NoError(t, err)
	assert.Len(t, conns, 13)

	// Postgres
	pg := conns["pg-local"]
	require.NotNil(t, pg)
	assert.Equal(t, "PG Local", pg.Name)
	assert.Equal(t, "postgres", pg.DialectType())
	require.NotNil(t, pg.Postgres)
	assert.Equal(t, "localhost", pg.Postgres.Host)
	assert.Equal(t, 54320, pg.Postgres.Port)
	assert.Equal(t, "postgres", pg.Postgres.Username)
	assert.True(t, pg.Postgres.AllowInsecure)

	// MySQL with params
	mysql := conns["mysql-prod"]
	require.NotNil(t, mysql)
	assert.Equal(t, "MySQL Production", mysql.Name)
	assert.Equal(t, 4, mysql.Parallelism)
	assert.Equal(t, "mysql", mysql.DialectType())
	require.NotNil(t, mysql.MySQL)
	assert.Equal(t, "utf8mb4", mysql.MySQL.Params["charset"])

	// BigQuery disabled
	bq := conns["bq-project"]
	require.NotNil(t, bq)
	assert.True(t, bq.Disabled)
	assert.Equal(t, "bigquery", bq.DialectType())
	require.NotNil(t, bq.BigQuery)
	assert.Equal(t, "my-gcp-project", bq.BigQuery.ProjectId)
	assert.Equal(t, "keys/service-account.json", bq.BigQuery.ServiceAccountKeyFile)

	// Snowflake with private key file
	sf := conns["sf-main"]
	require.NotNil(t, sf)
	assert.Equal(t, "snowflake", sf.DialectType())
	require.NotNil(t, sf.Snowflake)
	assert.Equal(t, "SYNQ_USER", sf.Snowflake.Username)
	assert.Equal(t, "keys/snowflake.p8", sf.Snowflake.PrivateKeyFile)
	assert.Equal(t, []string{"ANALYTICS", "STAGING"}, sf.Snowflake.Databases)
	assert.True(t, sf.Snowflake.UseGetDdl)
	assert.Equal(t, "SNOWFLAKE", sf.Snowflake.AccountUsageDb)

	// Trino
	trino := conns["trino-galaxy"]
	require.NotNil(t, trino)
	assert.Equal(t, "trino", trino.DialectType())
	require.NotNil(t, trino.Trino)
	assert.Equal(t, []string{"tpch", "iceberg_gcs"}, trino.Trino.Catalogs)
	assert.True(t, trino.Trino.FetchTableComments)

	// Databricks with OAuth
	dbr := conns["databricks-prod"]
	require.NotNil(t, dbr)
	assert.Equal(t, "databricks", dbr.DialectType())
	require.NotNil(t, dbr.Databricks)
	assert.Equal(t, "abc123def456", dbr.Databricks.Warehouse)
	assert.True(t, dbr.Databricks.RefreshTableMetrics)
	assert.True(t, dbr.Databricks.FetchTableTags)

	// MSSQL
	mssql := conns["mssql-azure"]
	require.NotNil(t, mssql)
	assert.Equal(t, "mssql", mssql.DialectType())
	require.NotNil(t, mssql.MSSQL)
	assert.Equal(t, 1433, mssql.MSSQL.Port)
	assert.True(t, mssql.MSSQL.TrustCert)
	assert.Equal(t, "true", mssql.MSSQL.Encrypt)

	// Oracle
	ora := conns["oracle-prod"]
	require.NotNil(t, ora)
	assert.Equal(t, "oracle", ora.DialectType())
	require.NotNil(t, ora.Oracle)
	assert.Equal(t, "ORCL", ora.Oracle.ServiceName)
	assert.True(t, ora.Oracle.SSL)
	assert.True(t, ora.Oracle.SSLVerify)
	assert.True(t, ora.Oracle.UseDiagnosticsPack)

	// DuckDB local
	duck := conns["duckdb-local"]
	require.NotNil(t, duck)
	assert.Equal(t, "duckdb", duck.DialectType())
	require.NotNil(t, duck.DuckDB)
	assert.Equal(t, ":memory:", duck.DuckDB.Database)

	// MotherDuck
	md := conns["motherduck"]
	require.NotNil(t, md)
	require.NotNil(t, md.DuckDB)
	assert.Equal(t, "my-org", md.DuckDB.MotherduckAccount)

	// Redshift
	rs := conns["redshift-prod"]
	require.NotNil(t, rs)
	assert.Equal(t, "redshift", rs.DialectType())
	require.NotNil(t, rs.Redshift)
	assert.Equal(t, 5439, rs.Redshift.Port)
	assert.True(t, rs.Redshift.FreshnessFromQueryLogs)

	// Clickhouse
	ch := conns["ch-staging"]
	require.NotNil(t, ch)
	assert.Equal(t, "clickhouse", ch.DialectType())
	require.NotNil(t, ch.Clickhouse)
	assert.Equal(t, 9440, ch.Clickhouse.Port)
}

func TestParseConnections_EnvExpansion(t *testing.T) {
	data, err := os.ReadFile("testdata/env_port.yaml")
	require.NoError(t, err)

	t.Setenv("PG_HOST", "db.example.com")
	t.Setenv("PG_PORT", "5433")
	t.Setenv("PG_USER", "testuser")
	t.Setenv("PG_PASS", "s3cret:with:colons")
	t.Setenv("PG_DB", "testdb")

	conns, err := ParseConnections(data, ParseOptions{ExpandEnv: true})
	require.NoError(t, err)

	pg := conns["pg-env"]
	require.NotNil(t, pg)
	require.NotNil(t, pg.Postgres)
	assert.Equal(t, "db.example.com", pg.Postgres.Host)
	assert.Equal(t, 5433, pg.Postgres.Port)
	assert.Equal(t, "testuser", pg.Postgres.Username)
	assert.Equal(t, "s3cret:with:colons", pg.Postgres.Password)
	assert.Equal(t, "testdb", pg.Postgres.Database)
}

func TestParseConnections_NoEnvExpansion_StringFields(t *testing.T) {
	// When env expansion is off, ${VAR} in string fields is preserved as literal
	data := []byte(`
connections:
  "pg-env":
    postgres:
      host: ${PG_HOST}
      port: 5432
      username: ${PG_USER}
      password: ${PG_PASS}
      database: ${PG_DB}
`)

	t.Setenv("PG_HOST", "should-not-appear")

	conns, err := ParseConnections(data, ParseOptions{ExpandEnv: false})
	require.NoError(t, err)

	pg := conns["pg-env"]
	require.NotNil(t, pg)
	require.NotNil(t, pg.Postgres)
	assert.Equal(t, "${PG_HOST}", pg.Postgres.Host)
	assert.Equal(t, "${PG_USER}", pg.Postgres.Username)
}

func TestParseConnections_NoEnvExpansion_IntFieldWithEnvVar(t *testing.T) {
	// When env expansion is off and a non-string field uses ${VAR}, parsing fails
	data, err := os.ReadFile("testdata/env_port.yaml")
	require.NoError(t, err)

	_, err = ParseConnections(data, ParseOptions{ExpandEnv: false})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unmarshal")
}

func TestParseConnections_NoEnvExpansionPreservesPlainValues(t *testing.T) {
	data, err := os.ReadFile("testdata/no_env.yaml")
	require.NoError(t, err)

	conns, err := ParseConnections(data, ParseOptions{ExpandEnv: false})
	require.NoError(t, err)

	pg := conns["pg-plain"]
	require.NotNil(t, pg)
	assert.Equal(t, "Plain Postgres", pg.Name)
	require.NotNil(t, pg.Postgres)
	assert.Equal(t, "localhost", pg.Postgres.Host)
	assert.Equal(t, 5432, pg.Postgres.Port)
	assert.Equal(t, "plainpassword", pg.Postgres.Password)
}

func TestResolveFiles_Snowflake(t *testing.T) {
	conns := map[string]*Connection{
		"sf": {
			Snowflake: &SnowflakeConf{
				Account:        "test",
				Warehouse:      "WH",
				Role:           "ROLE",
				Username:       "USER",
				PrivateKeyFile: "keys/snowflake.p8",
			},
		},
	}

	opts := ParseOptions{
		BaseDir: "/etc/synq",
		ReadFile: func(path string) ([]byte, error) {
			assert.Equal(t, "/etc/synq/keys/snowflake.p8", path)
			return []byte("PRIVATE_KEY_CONTENT"), nil
		},
	}

	err := ResolveFiles(conns, opts)
	require.NoError(t, err)
	assert.Equal(t, "PRIVATE_KEY_CONTENT", conns["sf"].Snowflake.PrivateKey)
	// File path is cleared so proto validators ("either inline or file, not both") pass.
	assert.Equal(t, "", conns["sf"].Snowflake.PrivateKeyFile)
}

func TestResolveFiles_BigQuery(t *testing.T) {
	conns := map[string]*Connection{
		"bq": {
			BigQuery: &BigQueryConf{
				ProjectId:             "my-project",
				Region:                "us-central1",
				ServiceAccountKeyFile: "service-account.json",
			},
		},
	}

	opts := ParseOptions{
		BaseDir: "/home/user/config",
		ReadFile: func(path string) ([]byte, error) {
			assert.Equal(t, "/home/user/config/service-account.json", path)
			return []byte(`{"type":"service_account"}`), nil
		},
	}

	err := ResolveFiles(conns, opts)
	require.NoError(t, err)
	assert.Equal(t, `{"type":"service_account"}`, conns["bq"].BigQuery.ServiceAccountKey)
	assert.Equal(t, "", conns["bq"].BigQuery.ServiceAccountKeyFile)
}

func TestResolveFiles_AbsolutePath(t *testing.T) {
	conns := map[string]*Connection{
		"sf": {
			Snowflake: &SnowflakeConf{
				Account:        "test",
				Warehouse:      "WH",
				Role:           "ROLE",
				Username:       "USER",
				PrivateKeyFile: "/absolute/path/key.p8",
			},
		},
	}

	opts := ParseOptions{
		BaseDir: "/should/not/matter",
		ReadFile: func(path string) ([]byte, error) {
			assert.Equal(t, "/absolute/path/key.p8", path)
			return []byte("KEY"), nil
		},
	}

	err := ResolveFiles(conns, opts)
	require.NoError(t, err)
	assert.Equal(t, "KEY", conns["sf"].Snowflake.PrivateKey)
	assert.Equal(t, "", conns["sf"].Snowflake.PrivateKeyFile)
}

func TestResolveFiles_InlineFieldTakesPrecedence(t *testing.T) {
	conns := map[string]*Connection{
		"sf": {
			Snowflake: &SnowflakeConf{
				Account:        "test",
				Warehouse:      "WH",
				Role:           "ROLE",
				Username:       "USER",
				PrivateKey:     "ALREADY_SET",
				PrivateKeyFile: "should-not-be-read.p8",
			},
		},
	}

	opts := ParseOptions{
		BaseDir: "/tmp",
		ReadFile: func(path string) ([]byte, error) {
			t.Fatal("ReadFile should not be called when inline field is already set")
			return nil, nil
		},
	}

	err := ResolveFiles(conns, opts)
	require.NoError(t, err)
	assert.Equal(t, "ALREADY_SET", conns["sf"].Snowflake.PrivateKey)
}

func TestResolveFiles_NilReadFile(t *testing.T) {
	conns := map[string]*Connection{
		"sf": {
			Snowflake: &SnowflakeConf{
				Account:        "test",
				Warehouse:      "WH",
				Role:           "ROLE",
				Username:       "USER",
				PrivateKeyFile: "key.p8",
			},
		},
	}

	// When ReadFile is nil, file fields are left as-is
	err := ResolveFiles(conns, ParseOptions{})
	require.NoError(t, err)
	assert.Equal(t, "", conns["sf"].Snowflake.PrivateKey)
	assert.Equal(t, "key.p8", conns["sf"].Snowflake.PrivateKeyFile)
}

func TestResolveFiles_CloudDenied(t *testing.T) {
	conns := map[string]*Connection{
		"sf": {
			Snowflake: &SnowflakeConf{
				Account:        "test",
				Warehouse:      "WH",
				Role:           "ROLE",
				Username:       "USER",
				PrivateKeyFile: "key.p8",
			},
		},
	}

	opts := ParseOptions{
		BaseDir: "/tmp",
		ReadFile: func(path string) ([]byte, error) {
			return nil, errors.New("file access not allowed")
		},
	}

	err := ResolveFiles(conns, opts)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "file access not allowed")
}

func TestParse_InvalidYAML(t *testing.T) {
	data := []byte("invalid: yaml: [")
	_, err := ParseConnections(data, ParseOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "yamlconfig")
}

func TestParse_NoConnections(t *testing.T) {
	data := []byte("agent:\n  log_level: debug\n")
	_, err := ParseConnections(data, ParseOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no connections found")
}

func TestParse_EmbeddedInLargerConfig(t *testing.T) {
	// Simulates synq-dwh or synq-scout style config
	type AppConfig struct {
		Agent struct {
			LogLevel string `yaml:"log_level"`
		} `yaml:"agent"`
		Connections map[string]*Connection `yaml:"connections"`
	}

	data := []byte(`
agent:
  log_level: debug
connections:
  "test-pg":
    postgres:
      host: localhost
      port: 5432
      username: admin
      password: secret
      database: testdb
`)

	var cfg AppConfig
	err := Parse(data, &cfg, ParseOptions{})
	require.NoError(t, err)
	assert.Equal(t, "debug", cfg.Agent.LogLevel)
	assert.Len(t, cfg.Connections, 1)

	pg := cfg.Connections["test-pg"]
	require.NotNil(t, pg)
	assert.Equal(t, "postgres", pg.DialectType())
	assert.Equal(t, "localhost", pg.Postgres.Host)
}

func TestParseConnectionsOnly(t *testing.T) {
	data := []byte(`
"conn-1":
  postgres:
    host: localhost
    port: 5432
    username: user
    password: pass
    database: db
"conn-2":
  mysql:
    host: localhost
    port: 3306
    username: root
    password: rootpass
    database: mydb
`)

	conns, err := ParseConnectionsOnly(data, ParseOptions{})
	require.NoError(t, err)
	assert.Len(t, conns, 2)
	assert.NotNil(t, conns["conn-1"].Postgres)
	assert.NotNil(t, conns["conn-2"].MySQL)
}

func TestParseConnections_CustomLookupVar(t *testing.T) {
	data := []byte(`
connections:
  "pg":
    postgres:
      host: ${DB_HOST}
      port: ${DB_PORT}
      username: ${DB_USER}
      password: ${DB_PASS}
      database: mydb
`)

	secrets := map[string]string{
		"DB_HOST": "secrets-manager.example.com",
		"DB_PORT": "5433",
		"DB_USER": "admin",
		"DB_PASS": "from-vault",
	}

	conns, err := ParseConnections(data, ParseOptions{
		ExpandEnv: true,
		LookupVar: func(name string) (string, bool) {
			v, ok := secrets[name]
			return v, ok
		},
	})
	require.NoError(t, err)

	pg := conns["pg"]
	require.NotNil(t, pg)
	require.NotNil(t, pg.Postgres)
	assert.Equal(t, "secrets-manager.example.com", pg.Postgres.Host)
	assert.Equal(t, 5433, pg.Postgres.Port)
	assert.Equal(t, "admin", pg.Postgres.Username)
	assert.Equal(t, "from-vault", pg.Postgres.Password)
}

func TestParseConnections_LookupVarDoesNotFallbackToEnv(t *testing.T) {
	t.Setenv("DB_HOST", "from-env")

	data := []byte(`
connections:
  "pg":
    postgres:
      host: ${DB_HOST}
      port: 5432
      username: u
      password: p
      database: db
`)

	conns, err := ParseConnections(data, ParseOptions{
		ExpandEnv: true,
		LookupVar: func(name string) (string, bool) {
			return "", false // not found
		},
	})
	require.NoError(t, err)

	pg := conns["pg"]
	require.NotNil(t, pg)
	assert.Equal(t, "", pg.Postgres.Host, "should use LookupVar result, not os.Getenv")
}

func TestParseConnections_StrictEnvFailsOnMissing(t *testing.T) {
	data := []byte(`
connections:
  "pg":
    postgres:
      host: ${MISSING_VAR}
      port: 5432
      username: u
      password: p
      database: db
`)

	_, err := ParseConnections(data, ParseOptions{
		ExpandEnv: true,
		StrictEnv: true,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "MISSING_VAR")
	assert.Contains(t, err.Error(), "not found")
}

func TestParseConnections_StrictEnvWithDefault(t *testing.T) {
	data := []byte(`
connections:
  "pg":
    postgres:
      host: ${MISSING_VAR:-localhost}
      port: 5432
      username: u
      password: p
      database: db
`)

	conns, err := ParseConnections(data, ParseOptions{
		ExpandEnv: true,
		StrictEnv: true,
	})
	require.NoError(t, err, "should not fail when default is provided")

	pg := conns["pg"]
	require.NotNil(t, pg)
	assert.Equal(t, "localhost", pg.Postgres.Host)
}

func TestParseConnections_StrictEnvWithEmptyDefault(t *testing.T) {
	data := []byte(`
connections:
  "pg":
    postgres:
      host: localhost
      port: 5432
      username: u
      password: ${OPTIONAL_PASS:-}
      database: db
`)

	conns, err := ParseConnections(data, ParseOptions{
		ExpandEnv: true,
		StrictEnv: true,
	})
	require.NoError(t, err, "should not fail when empty default is provided via ${VAR:-}")

	pg := conns["pg"]
	require.NotNil(t, pg)
	assert.Equal(t, "", pg.Postgres.Password)
}

func TestParseConnections_DefaultValueUsed(t *testing.T) {
	data := []byte(`
connections:
  "pg":
    postgres:
      host: ${DB_HOST:-fallback.example.com}
      port: ${DB_PORT:-5432}
      username: u
      password: p
      database: db
`)

	conns, err := ParseConnections(data, ParseOptions{ExpandEnv: true})
	require.NoError(t, err)

	pg := conns["pg"]
	require.NotNil(t, pg)
	assert.Equal(t, "fallback.example.com", pg.Postgres.Host)
	assert.Equal(t, 5432, pg.Postgres.Port)
}

func TestParseConnections_DefaultValueOverriddenByEnv(t *testing.T) {
	t.Setenv("DB_HOST", "real-host.example.com")

	data := []byte(`
connections:
  "pg":
    postgres:
      host: ${DB_HOST:-fallback.example.com}
      port: 5432
      username: u
      password: p
      database: db
`)

	conns, err := ParseConnections(data, ParseOptions{ExpandEnv: true})
	require.NoError(t, err)

	pg := conns["pg"]
	require.NotNil(t, pg)
	assert.Equal(t, "real-host.example.com", pg.Postgres.Host)
}

func TestApplyDefaults_SetsParallelism(t *testing.T) {
	conns := map[string]*Connection{
		"a": {Parallelism: 0, Postgres: &PostgresConf{Host: "localhost"}},
		"b": {Parallelism: 16, Postgres: &PostgresConf{Host: "localhost"}},
		"c": {Parallelism: 0, MySQL: &MySQLConf{Host: "localhost"}},
	}

	ApplyDefaults(conns)

	assert.Equal(t, 8, conns["a"].Parallelism, "zero parallelism should default to 8")
	assert.Equal(t, 16, conns["b"].Parallelism, "explicit parallelism should be preserved")
	assert.Equal(t, 8, conns["c"].Parallelism, "zero parallelism should default to 8")
}

func TestApplyDefaults_EmptyMap(t *testing.T) {
	conns := map[string]*Connection{}
	ApplyDefaults(conns) // should not panic
	assert.Empty(t, conns)
}

func TestCLIOptions(t *testing.T) {
	opts := CLIOptions("/etc/synq/agent.yaml")
	assert.True(t, opts.ExpandEnv)
	assert.Equal(t, "/etc/synq", opts.BaseDir)
	assert.NotNil(t, opts.ReadFile)
}

func TestResolveFiles_E2EWithRealFile(t *testing.T) {
	dir := t.TempDir()

	// Write a key file
	keyPath := filepath.Join(dir, "test.key")
	require.NoError(t, os.WriteFile(keyPath, []byte("MY_KEY_DATA"), 0o600))

	// Write config referencing relative key path
	configData := []byte(`
connections:
  "sf":
    snowflake:
      account: test
      warehouse: WH
      role: ROLE
      username: USER
      private_key_file: test.key
`)

	conns, err := ParseConnections(configData, ParseOptions{
		BaseDir:  dir,
		ReadFile: os.ReadFile,
	})
	require.NoError(t, err)
	assert.Equal(t, "MY_KEY_DATA", conns["sf"].Snowflake.PrivateKey)
}
