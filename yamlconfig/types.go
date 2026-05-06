// Package yamlconfig provides YAML-based DWH connection config parsing and proto conversion.
package yamlconfig

import (
	"os"
	"path/filepath"

	"github.com/invopop/jsonschema"
)

// Connection is a single database connection entry in a connections map.
// The map key serves as the connection ID.
// Exactly one of the database-type fields must be set.
type Connection struct {
	// Display name for this connection. Defaults to the connection ID (map key).
	Name string `yaml:"name,omitempty"`
	// When true, the connection is skipped during execution.
	Disabled bool `yaml:"disabled,omitempty"`
	// Maximum number of parallel queries. Range: 1-256. Defaults to 8.
	Parallelism int `yaml:"parallelism,omitempty" jsonschema:"minimum=1,maximum=256"`

	Postgres   *PostgresConf   `yaml:"postgres,omitempty"`
	Snowflake  *SnowflakeConf  `yaml:"snowflake,omitempty"`
	BigQuery   *BigQueryConf   `yaml:"bigquery,omitempty"`
	Redshift   *RedshiftConf   `yaml:"redshift,omitempty"`
	MySQL      *MySQLConf      `yaml:"mysql,omitempty"`
	Clickhouse *ClickhouseConf `yaml:"clickhouse,omitempty"`
	Trino      *TrinoConf      `yaml:"trino,omitempty"`
	Databricks *DatabricksConf `yaml:"databricks,omitempty"`
	MSSQL      *MSSQLConf      `yaml:"mssql,omitempty"`
	Oracle     *OracleConf     `yaml:"oracle,omitempty"`
	DuckDB     *DuckDBConf     `yaml:"duckdb,omitempty"`
	Athena     *AthenaConf     `yaml:"athena,omitempty"`
}

// DialectType returns the warehouse type string for this connection, or empty if none is set.
func (c *Connection) DialectType() string {
	switch {
	case c.Postgres != nil:
		return "postgres"
	case c.Snowflake != nil:
		return "snowflake"
	case c.BigQuery != nil:
		return "bigquery"
	case c.Redshift != nil:
		return "redshift"
	case c.MySQL != nil:
		return "mysql"
	case c.Clickhouse != nil:
		return "clickhouse"
	case c.Trino != nil:
		return "trino"
	case c.Databricks != nil:
		return "databricks"
	case c.MSSQL != nil:
		return "mssql"
	case c.Oracle != nil:
		return "oracle"
	case c.DuckDB != nil:
		return "duckdb"
	case c.Athena != nil:
		return "athena"
	default:
		return ""
	}
}

// PostgresConf contains PostgreSQL connection parameters.
type PostgresConf struct {
	Host     string `yaml:"host"           jsonschema:"required"`
	Port     int    `yaml:"port,omitempty" jsonschema:"minimum=1,maximum=65535"`
	Database string `yaml:"database"       jsonschema:"required"`
	Username string `yaml:"username"       jsonschema:"required"`
	Password string `yaml:"password"       jsonschema:"required"`
	// Disable SSL certificate verification.
	AllowInsecure bool `yaml:"allow_insecure,omitempty"`
}

// SnowflakeConf contains Snowflake connection parameters.
// Authentication: provide password, private_key/private_key_file, or set auth_type to "externalbrowser".
type SnowflakeConf struct {
	// Snowflake account identifier.
	Account string `yaml:"account" jsonschema:"required"`
	// Virtual warehouse to use for queries.
	Warehouse string `yaml:"warehouse" jsonschema:"required"`
	// Role to assume after connecting.
	Role     string `yaml:"role"               jsonschema:"required"`
	Username string `yaml:"username"           jsonschema:"required"`
	Password string `yaml:"password,omitempty"`
	// PEM-encoded private key content for key-pair authentication.
	PrivateKey string `yaml:"private_key,omitempty"`
	// Path to a PEM-encoded private key file.
	PrivateKeyFile string `yaml:"private_key_file,omitempty"`
	// Passphrase to decrypt the private key.
	PrivateKeyPassphrase string `yaml:"private_key_passphrase,omitempty"`
	// Databases to include. If empty, all accessible databases are scraped.
	Databases []string `yaml:"databases,omitempty"`
	// Use GET_DDL() to retrieve DDL for tables and views.
	UseGetDdl bool `yaml:"use_get_ddl,omitempty"`
	// Database containing the ACCOUNT_USAGE schema. Defaults to SNOWFLAKE.
	AccountUsageDb string `yaml:"account_usage_db,omitempty"`
	// Set to "externalbrowser" to use SSO browser-based authentication.
	AuthType string `yaml:"auth_type,omitempty"`
}

// fileFields returns a list of (pointer to file field, pointer to inline field) pairs
// that should be resolved by ReadFile.
func (c *SnowflakeConf) fileFields() []fileFieldPair {
	return []fileFieldPair{
		{fileField: &c.PrivateKeyFile, inlineField: &c.PrivateKey},
	}
}

// BigQueryConf contains BigQuery connection parameters.
// Exactly one of service_account_key or service_account_key_file should be set.
type BigQueryConf struct {
	// GCP project ID.
	ProjectId string `yaml:"project_id" jsonschema:"required"`
	// Region for BigQuery resources.
	Region string `yaml:"region" jsonschema:"required"`
	// Inline JSON content of the service account key.
	ServiceAccountKey string `yaml:"service_account_key,omitempty"`
	// Path to the service account key JSON file.
	ServiceAccountKeyFile string `yaml:"service_account_key_file,omitempty"`
	// Explicit list of dataset names to scrape. When set, only these datasets are queried
	// and project-level bigquery.datasets.list permission is not required.
	Datasets []string `yaml:"datasets,omitempty"`
}

// fileFields returns file field pairs for BigQuery.
func (c *BigQueryConf) fileFields() []fileFieldPair {
	return []fileFieldPair{
		{fileField: &c.ServiceAccountKeyFile, inlineField: &c.ServiceAccountKey},
	}
}

// RedshiftConf contains Amazon Redshift connection parameters.
type RedshiftConf struct {
	Host     string `yaml:"host"     jsonschema:"required"`
	Port     int    `yaml:"port"     jsonschema:"required,minimum=1,maximum=65535"`
	Database string `yaml:"database" jsonschema:"required"`
	Username string `yaml:"username" jsonschema:"required"`
	Password string `yaml:"password" jsonschema:"required"`
	// Estimate table freshness from Redshift query logs instead of metadata.
	FreshnessFromQueryLogs bool `yaml:"freshness_from_query_logs,omitempty"`
}

// MySQLConf contains MySQL connection parameters.
type MySQLConf struct {
	Host     string `yaml:"host"               jsonschema:"required"`
	Port     int    `yaml:"port"               jsonschema:"required,minimum=1,maximum=65535"`
	Database string `yaml:"database,omitempty"`
	Username string `yaml:"username"           jsonschema:"required"`
	Password string `yaml:"password"           jsonschema:"required"`
	// Disable SSL certificate verification.
	AllowInsecure bool `yaml:"allow_insecure,omitempty"`
	// Additional DSN parameters passed to the driver.
	Params map[string]string `yaml:"params,omitempty"`
}

// ClickhouseConf contains ClickHouse connection parameters.
type ClickhouseConf struct {
	Host string `yaml:"host"           jsonschema:"required"`
	Port int    `yaml:"port,omitempty" jsonschema:"minimum=1,maximum=65535"`
	// Database to connect to. If empty, all databases are scraped.
	Database string `yaml:"database,omitempty"`
	Username string `yaml:"username"           jsonschema:"required"`
	Password string `yaml:"password"           jsonschema:"required"`
	// Disable SSL certificate verification.
	AllowInsecure bool `yaml:"allow_insecure,omitempty"`
}

// TrinoConf contains Trino / Starburst connection parameters.
type TrinoConf struct {
	Host string `yaml:"host"           jsonschema:"required"`
	Port int    `yaml:"port,omitempty" jsonschema:"minimum=1,maximum=65535"`
	// Use a plain HTTP connection instead of HTTPS.
	UsePlaintext bool   `yaml:"use_plaintext,omitempty"`
	Username     string `yaml:"username,omitempty"`
	Password     string `yaml:"password,omitempty"`
	// Catalogs to include. Required for most Trino deployments.
	Catalogs            []string `yaml:"catalogs,omitempty"`
	NoShowCreateView    bool     `yaml:"no_show_create_view,omitempty"`
	NoShowCreateTable   bool     `yaml:"no_show_create_table,omitempty"`
	NoMaterializedViews bool     `yaml:"no_materialized_views,omitempty"`
	FetchTableComments  bool     `yaml:"fetch_table_comments,omitempty"`
}

// DatabricksConf contains Databricks connection parameters.
// Authentication: set auth_token, or set both auth_client and auth_secret.
type DatabricksConf struct {
	// Databricks workspace URL.
	WorkspaceUrl string `yaml:"workspace_url" jsonschema:"required"`
	// Personal access token for authentication.
	AuthToken string `yaml:"auth_token,omitempty"`
	// OAuth client ID (M2M authentication).
	AuthClient string `yaml:"auth_client,omitempty"`
	// OAuth client secret (M2M authentication).
	AuthSecret string `yaml:"auth_secret,omitempty"`
	// SQL warehouse ID to use for queries.
	Warehouse                  string `yaml:"warehouse,omitempty"`
	RefreshTableMetrics        bool   `yaml:"refresh_table_metrics,omitempty"`
	RefreshTableMetricsUseScan bool   `yaml:"refresh_table_metrics_use_scan,omitempty"`
	FetchTableTags             bool   `yaml:"fetch_table_tags,omitempty"`
	UseShowCreateTable         bool   `yaml:"use_show_create_table,omitempty"`
}

// MSSQLConf contains Microsoft SQL Server / Azure SQL Database connection parameters.
type MSSQLConf struct {
	Host     string `yaml:"host"               jsonschema:"required"`
	Port     int    `yaml:"port,omitempty"     jsonschema:"minimum=1,maximum=65535"`
	Database string `yaml:"database"           jsonschema:"required"`
	Username string `yaml:"username,omitempty"`
	Password string `yaml:"password,omitempty"`
	// Trust the server certificate without validation.
	TrustCert bool `yaml:"trust_cert,omitempty"`
	// Encryption mode (e.g. "true", "false", "strict").
	Encrypt string `yaml:"encrypt,omitempty"`
	// Federated authentication method (e.g. "ActiveDirectoryDefault").
	FedAuth string `yaml:"fed_auth,omitempty"`
	// Pre-acquired access token for Azure AD authentication.
	AccessToken string `yaml:"access_token,omitempty"`
	// Azure AD application client ID for service principal auth.
	ApplicationClientId string `yaml:"application_client_id,omitempty"`
}

// OracleConf contains Oracle Database connection parameters.
type OracleConf struct {
	Host string `yaml:"host"           jsonschema:"required"`
	Port int    `yaml:"port,omitempty" jsonschema:"minimum=1,maximum=65535"`
	// Oracle service name.
	ServiceName string `yaml:"service_name"       jsonschema:"required"`
	Username    string `yaml:"username,omitempty"`
	Password    string `yaml:"password,omitempty"`
	// Enable SSL/TLS for the connection.
	SSL bool `yaml:"ssl,omitempty"`
	// Verify the server's SSL certificate.
	SSLVerify bool `yaml:"ssl_verify,omitempty"`
	// Path to Oracle Wallet directory for authentication.
	WalletPath string `yaml:"wallet_path,omitempty"`
	// Enable Oracle Diagnostics Pack features (AWR, ASH).
	UseDiagnosticsPack bool `yaml:"use_diagnostics_pack,omitempty"`
}

// DuckDBConf contains DuckDB / MotherDuck connection parameters.
type DuckDBConf struct {
	// File path, ':memory:' for in-memory, or MotherDuck database name.
	Database string `yaml:"database,omitempty"`
	// MotherDuck organization/account name (for cloud mode).
	MotherduckAccount string `yaml:"motherduck_account,omitempty"`
	// MotherDuck authentication token (required for cloud MotherDuck).
	MotherduckToken string `yaml:"motherduck_token,omitempty"`
}

// AthenaConf contains Amazon Athena connection parameters. Authentication
// resolves in priority order: explicit access_key_id + secret_access_key,
// then aws_profile, then the agent host's default AWS credential chain
// (env vars, shared config, EC2/ECS/EKS instance role). When role_arn is set
// the resolved base credentials are wrapped in an STS AssumeRole provider.
type AthenaConf struct {
	// AWS region hosting the Athena service and Glue Data Catalog.
	Region string `yaml:"region" jsonschema:"required"`
	// Athena workgroup. Defaults to "primary" when empty. Must have a
	// ResultConfiguration.OutputLocation configured.
	Workgroup string `yaml:"workgroup,omitempty"`
	// Glue Data Catalog name. Defaults to "AwsDataCatalog" when empty.
	Catalog string `yaml:"catalog,omitempty"`

	// Static AWS credentials. Pair access_key_id with secret_access_key.
	AccessKeyID     string `yaml:"access_key_id,omitempty"`
	SecretAccessKey string `yaml:"secret_access_key,omitempty"`
	// Optional STS session token, when access_key_id+secret_access_key are
	// short-lived STS credentials.
	SessionToken string `yaml:"session_token,omitempty"`
	// Named AWS shared-config profile (from ~/.aws/credentials or
	// ~/.aws/config). Used only when static credentials are absent.
	AwsProfile string `yaml:"aws_profile,omitempty"`

	// IAM role ARN to assume via STS. Wraps whichever base credentials
	// resolved above (or the host's default chain when no other auth is set).
	RoleArn string `yaml:"role_arn,omitempty"`
	// External ID required by the role's trust policy. Pair with role_arn.
	ExternalID string `yaml:"external_id,omitempty"`
	// Optional STS session name. Defaults to "synq-athena".
	RoleSessionName string `yaml:"role_session_name,omitempty"`

	// Scope filter for include/exclude filtering by Glue catalog, Glue
	// database, and table. Mapping: ScopeRule.database = Glue catalog,
	// ScopeRule.schema = Glue database, ScopeRule.table = Glue table/view.
	Scope *ScopeConf `yaml:"scope,omitempty"`

	// Use SHOW CREATE TABLE to retrieve full table DDL (CTAS bodies, Iceberg
	// TBLPROPERTIES, Hive external LOCATION/SerDe). One Athena query per
	// table — billed at the 10MB scan minimum each.
	UseShowCreateTable bool `yaml:"use_show_create_table,omitempty"`
	// Use SHOW CREATE VIEW to retrieve full view DDL instead of the
	// rewritten body from information_schema.views.view_definition.
	UseShowCreateView bool `yaml:"use_show_create_view,omitempty"`
	// For Iceberg tables, fan out one Athena query per table to read row
	// count, total file size, snapshot commit timestamp, and partition
	// columns from the table's $files / $snapshots / $partitions metadata
	// tables. Hive externals are unaffected.
	UseIcebergMetricsScan bool `yaml:"use_iceberg_metrics_scan,omitempty"`
}

// ScopeConf is the YAML representation of synq.common.v1.ScopeFilter — the
// shared include/exclude filter used by warehouses with hierarchical catalogs.
type ScopeConf struct {
	// Include rules. If non-empty, only matching objects are accepted.
	Include []ScopeRuleConf `yaml:"include,omitempty"`
	// Exclude rules. Matching objects are rejected, even if they match an include rule.
	Exclude []ScopeRuleConf `yaml:"exclude,omitempty"`
}

// ScopeRuleConf is a single include/exclude rule. Empty fields match anything;
// '*' acts as a glob wildcard. Matching is case-insensitive.
type ScopeRuleConf struct {
	// Database-level pattern (catalog for Athena/Trino/Databricks, project for BigQuery).
	Database string `yaml:"database,omitempty"`
	// Schema-level pattern (Glue database for Athena, dataset for BigQuery).
	Schema string `yaml:"schema,omitempty"`
	// Table or view name pattern.
	Table string `yaml:"table,omitempty"`
}

// ConnectionsSchema returns a JSON schema for a map of connections,
// suitable for embedding in a larger config schema.
func ConnectionsSchema() *jsonschema.Schema {
	r := NewReflector()
	type wrapper struct {
		Connections map[string]*Connection `yaml:"connections" jsonschema:"required,minProperties=1"`
	}
	return r.Reflect(&wrapper{})
}

// ReflectorOption configures a jsonschema.Reflector.
type ReflectorOption func(*jsonschema.Reflector)

// WithGoComments adds Go doc comments from a package as JSON schema descriptions.
// The base is the import path and srcDir is the local filesystem path to the source
// (can be relative to the working directory).
// Errors are silently ignored (source may not be available in CI or production).
func WithGoComments(base, srcDir string) ReflectorOption {
	return func(r *jsonschema.Reflector) {
		// AddGoComments joins base + path via path.Join to build comment map keys.
		// When path is ".", the key becomes just "base" which matches fullyQualifiedTypeName.
		// For relative or absolute paths, we need to chdir to the source directory
		// so we can pass "." as the path.
		abs, err := filepath.Abs(srcDir)
		if err != nil {
			return
		}
		cwd, err := os.Getwd()
		if err != nil {
			return
		}
		if err := os.Chdir(abs); err != nil {
			return
		}
		_ = r.AddGoComments(base, ".")
		_ = os.Chdir(cwd)
	}
}

// NewReflector returns a jsonschema.Reflector configured for YAML config structs.
// Use WithGoComments or WithYAMLConfigComments to add descriptions from Go doc comments.
func NewReflector(opts ...ReflectorOption) *jsonschema.Reflector {
	r := &jsonschema.Reflector{
		ExpandedStruct: true,
		FieldNameTag:   "yaml",
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}
