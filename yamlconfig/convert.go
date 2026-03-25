package yamlconfig

import (
	"fmt"

	agentdwhv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/agent/dwh/v1"
)

// ToProtoConnections converts all YAML connections to proto Connection messages.
// Connection IDs are used as the map keys and as default names when name is empty.
func ToProtoConnections(conns map[string]*Connection) (map[string]*agentdwhv1.Connection, error) {
	result := make(map[string]*agentdwhv1.Connection, len(conns))
	for id, conn := range conns {
		proto, err := ToProtoConnection(id, conn)
		if err != nil {
			return nil, fmt.Errorf("connection %q: %w", id, err)
		}
		result[id] = proto
	}
	return result, nil
}

// ToProtoConnection converts a single YAML Connection to a proto Connection message.
func ToProtoConnection(id string, conn *Connection) (*agentdwhv1.Connection, error) {
	if conn == nil {
		return nil, fmt.Errorf("nil connection")
	}

	name := conn.Name
	if name == "" {
		name = id
	}

	proto := &agentdwhv1.Connection{
		Name:        name,
		Disabled:    conn.Disabled,
		Parallelism: int32(conn.Parallelism),
	}

	switch {
	case conn.Postgres != nil:
		proto.Config = &agentdwhv1.Connection_Postgres{
			Postgres: postgresConfToProto(conn.Postgres),
		}
	case conn.Snowflake != nil:
		proto.Config = &agentdwhv1.Connection_Snowflake{
			Snowflake: snowflakeConfToProto(conn.Snowflake),
		}
	case conn.BigQuery != nil:
		proto.Config = &agentdwhv1.Connection_Bigquery{
			Bigquery: bigqueryConfToProto(conn.BigQuery),
		}
	case conn.Redshift != nil:
		proto.Config = &agentdwhv1.Connection_Redshift{
			Redshift: redshiftConfToProto(conn.Redshift),
		}
	case conn.MySQL != nil:
		proto.Config = &agentdwhv1.Connection_Mysql{
			Mysql: mysqlConfToProto(conn.MySQL),
		}
	case conn.Clickhouse != nil:
		proto.Config = &agentdwhv1.Connection_Clickhouse{
			Clickhouse: clickhouseConfToProto(conn.Clickhouse),
		}
	case conn.Trino != nil:
		proto.Config = &agentdwhv1.Connection_Trino{
			Trino: trinoConfToProto(conn.Trino),
		}
	case conn.Databricks != nil:
		proto.Config = &agentdwhv1.Connection_Databricks{
			Databricks: databricksConfToProto(conn.Databricks),
		}
	case conn.MSSQL != nil:
		proto.Config = &agentdwhv1.Connection_Mssql{
			Mssql: mssqlConfToProto(conn.MSSQL),
		}
	case conn.Oracle != nil:
		proto.Config = &agentdwhv1.Connection_Oracle{
			Oracle: oracleConfToProto(conn.Oracle),
		}
	case conn.DuckDB != nil:
		proto.Config = &agentdwhv1.Connection_Duckdb{
			Duckdb: duckdbConfToProto(conn.DuckDB),
		}
	default:
		return nil, fmt.Errorf("no database type configured")
	}

	return proto, nil
}

func postgresConfToProto(c *PostgresConf) *agentdwhv1.PostgresConf {
	return &agentdwhv1.PostgresConf{
		Host:          c.Host,
		Port:          int32(c.Port),
		Database:      c.Database,
		Username:      c.Username,
		Password:      c.Password,
		AllowInsecure: c.AllowInsecure,
	}
}

func snowflakeConfToProto(c *SnowflakeConf) *agentdwhv1.SnowflakeConf {
	sf := &agentdwhv1.SnowflakeConf{
		Account:        c.Account,
		Warehouse:      c.Warehouse,
		Role:           c.Role,
		Username:       c.Username,
		Password:       c.Password,
		PrivateKey:     c.PrivateKey,
		PrivateKeyFile: c.PrivateKeyFile,
		Databases:      c.Databases,
		UseGetDdl:      c.UseGetDdl,
	}
	if c.PrivateKeyPassphrase != "" {
		sf.PrivateKeyPassphrase = &c.PrivateKeyPassphrase
	}
	if c.AccountUsageDb != "" {
		sf.AccountUsageDb = &c.AccountUsageDb
	}
	if c.AuthType != "" {
		sf.AuthType = &c.AuthType
	}
	return sf
}

func bigqueryConfToProto(c *BigQueryConf) *agentdwhv1.BigQueryConf {
	return &agentdwhv1.BigQueryConf{
		ProjectId:             c.ProjectId,
		Region:                c.Region,
		ServiceAccountKey:     c.ServiceAccountKey,
		ServiceAccountKeyFile: c.ServiceAccountKeyFile,
	}
}

func redshiftConfToProto(c *RedshiftConf) *agentdwhv1.RedshiftConf {
	return &agentdwhv1.RedshiftConf{
		Host:                   c.Host,
		Port:                   int32(c.Port),
		Database:               c.Database,
		Username:               c.Username,
		Password:               c.Password,
		FreshnessFromQueryLogs: c.FreshnessFromQueryLogs,
	}
}

func mysqlConfToProto(c *MySQLConf) *agentdwhv1.MySQLConf {
	return &agentdwhv1.MySQLConf{
		Host:          c.Host,
		Port:          int32(c.Port),
		Database:      c.Database,
		Username:      c.Username,
		Password:      c.Password,
		AllowInsecure: c.AllowInsecure,
		Params:        c.Params,
	}
}

func clickhouseConfToProto(c *ClickhouseConf) *agentdwhv1.ClickhouseConf {
	return &agentdwhv1.ClickhouseConf{
		Host:          c.Host,
		Port:          int32(c.Port),
		Database:      c.Database,
		Username:      c.Username,
		Password:      c.Password,
		AllowInsecure: c.AllowInsecure,
	}
}

func trinoConfToProto(c *TrinoConf) *agentdwhv1.TrinoConf {
	t := &agentdwhv1.TrinoConf{
		Host:                c.Host,
		Username:            c.Username,
		Password:            c.Password,
		Catalogs:            c.Catalogs,
		NoShowCreateView:    c.NoShowCreateView,
		NoShowCreateTable:   c.NoShowCreateTable,
		NoMaterializedViews: c.NoMaterializedViews,
		FetchTableComments:  c.FetchTableComments,
	}
	if c.Port != 0 {
		port := int32(c.Port)
		t.Port = &port
	}
	if c.UsePlaintext {
		t.UsePlaintext = &c.UsePlaintext
	}
	return t
}

func databricksConfToProto(c *DatabricksConf) *agentdwhv1.DatabricksConf {
	d := &agentdwhv1.DatabricksConf{
		WorkspaceUrl:               c.WorkspaceUrl,
		RefreshTableMetrics:        c.RefreshTableMetrics,
		RefreshTableMetricsUseScan: c.RefreshTableMetricsUseScan,
		FetchTableTags:             c.FetchTableTags,
		UseShowCreateTable:         c.UseShowCreateTable,
	}
	if c.AuthToken != "" {
		d.AuthToken = &c.AuthToken
	}
	if c.AuthClient != "" {
		d.AuthClient = &c.AuthClient
	}
	if c.AuthSecret != "" {
		d.AuthSecret = &c.AuthSecret
	}
	if c.Warehouse != "" {
		d.Warehouse = &c.Warehouse
	}
	return d
}

func mssqlConfToProto(c *MSSQLConf) *agentdwhv1.MSSQLConf {
	return &agentdwhv1.MSSQLConf{
		Host:                c.Host,
		Port:                int32(c.Port),
		Database:            c.Database,
		Username:            c.Username,
		Password:            c.Password,
		TrustCert:           c.TrustCert,
		Encrypt:             c.Encrypt,
		FedAuth:             c.FedAuth,
		AccessToken:         c.AccessToken,
		ApplicationClientId: c.ApplicationClientId,
	}
}

func oracleConfToProto(c *OracleConf) *agentdwhv1.OracleConf {
	return &agentdwhv1.OracleConf{
		Host:               c.Host,
		Port:               int32(c.Port),
		ServiceName:        c.ServiceName,
		Username:           c.Username,
		Password:           c.Password,
		Ssl:                c.SSL,
		SslVerify:          c.SSLVerify,
		WalletPath:         c.WalletPath,
		UseDiagnosticsPack: c.UseDiagnosticsPack,
	}
}

func duckdbConfToProto(c *DuckDBConf) *agentdwhv1.DuckDBConf {
	return &agentdwhv1.DuckDBConf{
		Database:          c.Database,
		MotherduckAccount: c.MotherduckAccount,
		MotherduckToken:   c.MotherduckToken,
	}
}
