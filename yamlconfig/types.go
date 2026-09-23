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
	Fabric     *FabricConf     `yaml:"fabric,omitempty"`
	Db2        *Db2Conf        `yaml:"db2,omitempty"`
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
	case c.Fabric != nil:
		return "fabric"
	case c.Db2 != nil:
		return "db2"
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
	// Name this ClickHouse is published under, and the top element of every path
	// and breadcrumb its tables appear in — "prod", "staging", "eu-analytics".
	//
	// ClickHouse has no container above a database, so something has to name the
	// service itself. Left empty, that is the host above, which is correct but
	// unreadable for a ClickHouse Cloud endpoint; a name given here replaces it.
	//
	// Two connections to the same service must give the same name, and the name is
	// the identity of everything scraped through it: changing it republishes those
	// tables under new paths, and the old ones stop being produced. Pick one per
	// service and keep it.
	InstanceName string `yaml:"instance_name,omitempty" jsonschema:"example=prod,example=staging"`
	// Database the connection opens with, so an unqualified table name in a query
	// resolves against it. It does not restrict what is scraped: metadata comes
	// from system tables and covers every database the user can see, whatever this
	// says. Empty opens on "default".
	Database string `yaml:"database,omitempty"`
	Username string `yaml:"username"           jsonschema:"required"`
	Password string `yaml:"password"           jsonschema:"required"`
	// Disable SSL certificate verification.
	AllowInsecure bool `yaml:"allow_insecure,omitempty"`
	// ClickHouse server settings applied to every connection, written as they
	// would be in a DSN query string (e.g. max_execution_time: "300"). Values are
	// typed the way ClickHouse types them in a connection string: "true" and
	// "false" become 1 and 0, whole numbers become integers, anything else is
	// passed through as text. A name given here replaces the value the scrape
	// would otherwise use.
	Settings map[string]string `yaml:"settings,omitempty"`
	// How ClickHouse system tables are read. Omit to keep reading across a
	// cluster named "default", which every ClickHouse Cloud service provides.
	Cluster *ClickhouseClusterConf `yaml:"cluster,omitempty"`
}

// Accepted values for ClickhouseClusterConf.Mode. Matching is case-insensitive
// and hyphens are accepted in place of underscores.
const (
	ClickhouseClusterModeAllReplicas = "all_replicas"
	ClickhouseClusterModeSingleNode  = "single_node"
)

// ClickhouseClusterConf selects how metadata reads address ClickHouse system
// tables.
//
// System tables are per-node, so on a service with more than one replica a plain
// read reflects whichever replica answered rather than the whole warehouse.
type ClickhouseClusterConf struct {
	// How system tables are read. Optional — defaults to "all_replicas".
	//   - "all_replicas": read through clusterAllReplicas(<name>, ...) so metadata
	//     covers every replica. Requires GRANT REMOTE ON *.*.
	//   - "single_node": read on the connected node only. The one setting that
	//     works on an install whose remote_servers defines no cluster, and it
	//     needs no REMOTE grant — but on an install that does have replicas it
	//     reports the metadata of a single node, so choose it deliberately.
	Mode string `yaml:"mode,omitempty" jsonschema:"example=all_replicas,example=single_node"`
	// Cluster to read through, as named under remote_servers in the ClickHouse
	// configuration. `SELECT DISTINCT cluster FROM system.clusters` lists what a
	// server has. Empty means "default". Ignored when mode is "single_node".
	Name string `yaml:"name,omitempty"`
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

// FabricConf contains the connection settings for a Microsoft Fabric Warehouse
// or Lakehouse SQL analytics endpoint. A single connection covers the whole
// Fabric workspace: every warehouse and lakehouse in it is exposed as a
// database. Encryption, the network port and the Microsoft Entra ID sign-in
// flow are handled automatically, so only the fields below need to be set.
//
// Authentication defaults to an Entra ID service principal — set client_id,
// client_secret and (optionally) tenant_id. Alternatively supply a pre-acquired
// access_token, or, for the on-prem agent only, set auth_type to sign in as the
// machine's own Azure identity.
type FabricConf struct {
	// Hostname of the workspace's SQL analytics endpoint. Copy it from the
	// Fabric portal: open your Warehouse or Lakehouse, then Settings → SQL
	// connection string.
	Host string `yaml:"host" jsonschema:"required,example=my-workspace.datawarehouse.fabric.microsoft.com"`
	// Default database for queries that don't name one explicitly. Optional —
	// defaults to "master". Because metadata and metric queries are always
	// fully qualified, this only affects ad-hoc SQL that omits the database.
	Database string `yaml:"database,omitempty" jsonschema:"example=my_warehouse"`
	// How to authenticate to Fabric. Optional — defaults to a service principal
	// (client_id + client_secret). Values are matched case-insensitively, and
	// the equivalent dbt-fabric and Microsoft ODBC spellings are also accepted:
	//   - "service_principal" (default): Entra ID service principal. Set
	//     client_id, client_secret and, if needed, tenant_id.
	//   - "azure_cli": reuse the machine's `az login` session. On-prem agent only.
	//   - "default": try Azure's default credential chain (environment, managed
	//     identity, CLI, ...). On-prem agent only.
	//   - "managed_identity": use an Azure managed identity; set client_id to
	//     select a user-assigned identity. On-prem agent only.
	AuthType string `yaml:"auth_type,omitempty" jsonschema:"example=service_principal,example=azure_cli,example=default,example=managed_identity"`
	// Application (client) ID of the Entra ID service principal. When auth_type
	// is "managed_identity", this instead selects a user-assigned identity.
	ClientId string `yaml:"client_id,omitempty" jsonschema:"example=00000000-0000-0000-0000-000000000000"`
	// Client secret for the service principal. Supply it through an environment
	// variable (e.g. ${FABRIC_CLIENT_SECRET}) rather than committing it in plain
	// text.
	ClientSecret string `yaml:"client_secret,omitempty" jsonschema:"example=${FABRIC_CLIENT_SECRET}"`
	// Entra ID tenant (directory) ID. Optional — inferred from the endpoint
	// hostname when omitted. Set it only when the service principal lives in a
	// different tenant than the workspace.
	TenantId string `yaml:"tenant_id,omitempty" jsonschema:"example=00000000-0000-0000-0000-000000000000"`
	// A pre-acquired Entra ID OAuth access token for the SQL scope
	// (https://database.windows.net/.default). Optional — when set it overrides
	// every other authentication method. Mainly for hosted deployments that mint
	// their own token.
	AccessToken string `yaml:"access_token,omitempty" jsonschema:"example=${FABRIC_ACCESS_TOKEN}"`
	// Optional include/exclude filter that limits which databases, schemas and
	// tables are scanned. When omitted, the whole workspace is scanned.
	Scope *ScopeConf `yaml:"scope,omitempty"`
}

// Db2Conf contains the connection settings for IBM Db2 for Linux, UNIX and
// Windows (Db2 LUW). Each connection opens one database, so add one per
// database you want to monitor. The keyword in parentheses after each field
// is the matching keyword of a Db2 CLI connection string, db2cli.ini or
// db2dsdriver.cfg, and takes the same values. No Db2 client or driver needs to
// be installed. Db2 for z/OS and Db2 for i are not supported.
type Db2Conf struct {
	// Host name or IP address of the Db2 server (HOSTNAME).
	Hostname string `yaml:"hostname" jsonschema:"required,example=db2.example.com"`
	// TCP/IP port of the instance (PORT): the SVCENAME database manager
	// configuration parameter, or SSL_SVCENAME when security is SSL. Optional,
	// defaults to 50000, or 50001 with SSL.
	Port int `yaml:"port,omitempty" jsonschema:"minimum=1,maximum=65535"`
	// Database name or alias (DATABASE), as in CONNECT TO <database>.
	Database string `yaml:"database" jsonschema:"required,example=SAMPLE"`
	// Authorization ID to connect with (UID). Db2 checks it against the
	// server's operating system, or LDAP when an LDAP plugin is configured.
	User string `yaml:"user" jsonschema:"required,example=db2inst1"`
	// Password of the authorization ID (PWD). Supply it through an environment
	// variable (e.g. ${DB2_PASSWORD}) rather than committing it in plain text.
	Password string `yaml:"password" jsonschema:"required,example=${DB2_PASSWORD}"`
	// Set to SSL to connect over SSL/TLS (SECURITY=SSL). Leave it out for a
	// plain TCP/IP connection.
	Security string `yaml:"security,omitempty" jsonschema:"enum=SSL,enum=ssl"`
	// Path to the server's certificate, or the certificate of the CA that
	// signed it, in PEM format (SSLServerCertificate), such as the .arm file
	// extracted from the instance's keystore with gsk8capicmd_64. Needs
	// security set to SSL. Leave it and ssl_server_certificate_pem out to check
	// the server against the system trust store.
	SSLServerCertificateFile string `yaml:"ssl_server_certificate_file,omitempty" jsonschema:"example=/opt/certs/db2server.arm"`
	// Content of the same certificate, starting with
	// -----BEGIN CERTIFICATE-----, for when you can't point to a file. Needs
	// security set to SSL.
	SSLServerCertificatePEM string `yaml:"ssl_server_certificate_pem,omitempty"`
	// How the authorization ID and password travel to the server
	// (AUTHENTICATION). SERVER (default) sends them unencrypted, so use it with
	// SSL on an untrusted network. SERVER_ENCRYPT encrypts them and needs the
	// instance's AUTHENTICATION to accept encrypted credentials.
	Authentication string `yaml:"authentication,omitempty" jsonschema:"enum=SERVER,enum=SERVER_ENCRYPT"`
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
