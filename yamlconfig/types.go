package yamlconfig

import "github.com/invopop/jsonschema"

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
	default:
		return ""
	}
}

// PostgresConf contains PostgreSQL connection parameters.
type PostgresConf struct {
	Host          string `yaml:"host"                     jsonschema:"required"`
	Port          int    `yaml:"port,omitempty"           jsonschema:"minimum=1,maximum=65535"`
	Database      string `yaml:"database"                 jsonschema:"required"`
	Username      string `yaml:"username"                 jsonschema:"required"`
	Password      string `yaml:"password"                 jsonschema:"required"`
	AllowInsecure bool   `yaml:"allow_insecure,omitempty"`
}

// SnowflakeConf contains Snowflake connection parameters.
// Authentication: provide password, private_key/private_key_file, or set auth_type to "externalbrowser".
type SnowflakeConf struct {
	Account              string   `yaml:"account"                          jsonschema:"required"`
	Warehouse            string   `yaml:"warehouse"                        jsonschema:"required"`
	Role                 string   `yaml:"role"                             jsonschema:"required"`
	Username             string   `yaml:"username"                         jsonschema:"required"`
	Password             string   `yaml:"password,omitempty"`
	PrivateKey           string   `yaml:"private_key,omitempty"`
	PrivateKeyFile       string   `yaml:"private_key_file,omitempty"`
	PrivateKeyPassphrase string   `yaml:"private_key_passphrase,omitempty"`
	Databases            []string `yaml:"databases,omitempty"`
	UseGetDdl            bool     `yaml:"use_get_ddl,omitempty"`
	AccountUsageDb       string   `yaml:"account_usage_db,omitempty"`
	AuthType             string   `yaml:"auth_type,omitempty"`
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
	ProjectId             string `yaml:"project_id"                         jsonschema:"required"`
	Region                string `yaml:"region"                             jsonschema:"required"`
	ServiceAccountKey     string `yaml:"service_account_key,omitempty"`
	ServiceAccountKeyFile string `yaml:"service_account_key_file,omitempty"`
}

// fileFields returns file field pairs for BigQuery.
func (c *BigQueryConf) fileFields() []fileFieldPair {
	return []fileFieldPair{
		{fileField: &c.ServiceAccountKeyFile, inlineField: &c.ServiceAccountKey},
	}
}

// RedshiftConf contains Amazon Redshift connection parameters.
type RedshiftConf struct {
	Host                   string `yaml:"host"                                jsonschema:"required"`
	Port                   int    `yaml:"port"                                jsonschema:"required,minimum=1,maximum=65535"`
	Database               string `yaml:"database"                            jsonschema:"required"`
	Username               string `yaml:"username"                            jsonschema:"required"`
	Password               string `yaml:"password"                            jsonschema:"required"`
	FreshnessFromQueryLogs bool   `yaml:"freshness_from_query_logs,omitempty"`
}

// MySQLConf contains MySQL connection parameters.
type MySQLConf struct {
	Host          string            `yaml:"host"                     jsonschema:"required"`
	Port          int               `yaml:"port"                     jsonschema:"required,minimum=1,maximum=65535"`
	Database      string            `yaml:"database,omitempty"`
	Username      string            `yaml:"username"                 jsonschema:"required"`
	Password      string            `yaml:"password"                 jsonschema:"required"`
	AllowInsecure bool              `yaml:"allow_insecure,omitempty"`
	Params        map[string]string `yaml:"params,omitempty"`
}

// ClickhouseConf contains ClickHouse connection parameters.
type ClickhouseConf struct {
	Host          string `yaml:"host"                     jsonschema:"required"`
	Port          int    `yaml:"port,omitempty"           jsonschema:"minimum=1,maximum=65535"`
	Database      string `yaml:"database,omitempty"`
	Username      string `yaml:"username"                 jsonschema:"required"`
	Password      string `yaml:"password"                 jsonschema:"required"`
	AllowInsecure bool   `yaml:"allow_insecure,omitempty"`
}

// TrinoConf contains Trino / Starburst connection parameters.
type TrinoConf struct {
	Host                string   `yaml:"host"                            jsonschema:"required"`
	Port                int      `yaml:"port,omitempty"                  jsonschema:"minimum=1,maximum=65535"`
	UsePlaintext        bool     `yaml:"use_plaintext,omitempty"`
	Username            string   `yaml:"username,omitempty"`
	Password            string   `yaml:"password,omitempty"`
	Catalogs            []string `yaml:"catalogs,omitempty"`
	NoShowCreateView    bool     `yaml:"no_show_create_view,omitempty"`
	NoShowCreateTable   bool     `yaml:"no_show_create_table,omitempty"`
	NoMaterializedViews bool     `yaml:"no_materialized_views,omitempty"`
	FetchTableComments  bool     `yaml:"fetch_table_comments,omitempty"`
}

// DatabricksConf contains Databricks connection parameters.
// Authentication: set auth_token, or set both auth_client and auth_secret.
type DatabricksConf struct {
	WorkspaceUrl               string `yaml:"workspace_url"                            jsonschema:"required"`
	AuthToken                  string `yaml:"auth_token,omitempty"`
	AuthClient                 string `yaml:"auth_client,omitempty"`
	AuthSecret                 string `yaml:"auth_secret,omitempty"`
	Warehouse                  string `yaml:"warehouse,omitempty"`
	RefreshTableMetrics        bool   `yaml:"refresh_table_metrics,omitempty"`
	RefreshTableMetricsUseScan bool   `yaml:"refresh_table_metrics_use_scan,omitempty"`
	FetchTableTags             bool   `yaml:"fetch_table_tags,omitempty"`
	UseShowCreateTable         bool   `yaml:"use_show_create_table,omitempty"`
}

// MSSQLConf contains Microsoft SQL Server / Azure SQL Database connection parameters.
type MSSQLConf struct {
	Host                string `yaml:"host"                            jsonschema:"required"`
	Port                int    `yaml:"port,omitempty"                  jsonschema:"minimum=1,maximum=65535"`
	Database            string `yaml:"database"                        jsonschema:"required"`
	Username            string `yaml:"username,omitempty"`
	Password            string `yaml:"password,omitempty"`
	TrustCert           bool   `yaml:"trust_cert,omitempty"`
	Encrypt             string `yaml:"encrypt,omitempty"`
	FedAuth             string `yaml:"fed_auth,omitempty"`
	AccessToken         string `yaml:"access_token,omitempty"`
	ApplicationClientId string `yaml:"application_client_id,omitempty"`
}

// OracleConf contains Oracle Database connection parameters.
type OracleConf struct {
	Host               string `yaml:"host"                           jsonschema:"required"`
	Port               int    `yaml:"port,omitempty"                 jsonschema:"minimum=1,maximum=65535"`
	ServiceName        string `yaml:"service_name"                   jsonschema:"required"`
	Username           string `yaml:"username,omitempty"`
	Password           string `yaml:"password,omitempty"`
	SSL                bool   `yaml:"ssl,omitempty"`
	SSLVerify          bool   `yaml:"ssl_verify,omitempty"`
	WalletPath         string `yaml:"wallet_path,omitempty"`
	UseDiagnosticsPack bool   `yaml:"use_diagnostics_pack,omitempty"`
}

// DuckDBConf contains DuckDB / MotherDuck connection parameters.
type DuckDBConf struct {
	Database          string `yaml:"database,omitempty"`
	MotherduckAccount string `yaml:"motherduck_account,omitempty"`
	MotherduckToken   string `yaml:"motherduck_token,omitempty"`
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

// NewReflector returns a jsonschema.Reflector configured for YAML config structs.
func NewReflector() *jsonschema.Reflector {
	return &jsonschema.Reflector{
		ExpandedStruct: true,
		FieldNameTag:   "yaml",
	}
}
