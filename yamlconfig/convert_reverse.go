package yamlconfig

import (
	agentdwhv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/agent/dwh/v1"
)

// FromProtoConnections converts proto Connection messages back to YAML Connection structs.
func FromProtoConnections(conns map[string]*agentdwhv1.Connection) map[string]*Connection {
	result := make(map[string]*Connection, len(conns))
	for id, proto := range conns {
		result[id] = FromProtoConnection(proto)
	}
	return result
}

// FromProtoConnection converts a single proto Connection to a YAML Connection struct.
func FromProtoConnection(proto *agentdwhv1.Connection) *Connection {
	if proto == nil {
		return nil
	}

	conn := &Connection{
		Name:        proto.GetName(),
		Disabled:    proto.GetDisabled(),
		Parallelism: int(proto.GetParallelism()),
	}

	switch t := proto.Config.(type) {
	case *agentdwhv1.Connection_Postgres:
		conn.Postgres = postgresConfFromProto(t.Postgres)
	case *agentdwhv1.Connection_Snowflake:
		conn.Snowflake = snowflakeConfFromProto(t.Snowflake)
	case *agentdwhv1.Connection_Bigquery:
		conn.BigQuery = bigqueryConfFromProto(t.Bigquery)
	case *agentdwhv1.Connection_Redshift:
		conn.Redshift = redshiftConfFromProto(t.Redshift)
	case *agentdwhv1.Connection_Mysql:
		conn.MySQL = mysqlConfFromProto(t.Mysql)
	case *agentdwhv1.Connection_Clickhouse:
		conn.Clickhouse = clickhouseConfFromProto(t.Clickhouse)
	case *agentdwhv1.Connection_Trino:
		conn.Trino = trinoConfFromProto(t.Trino)
	case *agentdwhv1.Connection_Databricks:
		conn.Databricks = databricksConfFromProto(t.Databricks)
	case *agentdwhv1.Connection_Mssql:
		conn.MSSQL = mssqlConfFromProto(t.Mssql)
	case *agentdwhv1.Connection_Oracle:
		conn.Oracle = oracleConfFromProto(t.Oracle)
	case *agentdwhv1.Connection_Duckdb:
		conn.DuckDB = duckdbConfFromProto(t.Duckdb)
	}

	return conn
}

func postgresConfFromProto(c *agentdwhv1.PostgresConf) *PostgresConf {
	if c == nil {
		return nil
	}
	return &PostgresConf{
		Host:          c.GetHost(),
		Port:          int(c.GetPort()),
		Database:      c.GetDatabase(),
		Username:      c.GetUsername(),
		Password:      c.GetPassword(),
		AllowInsecure: c.GetAllowInsecure(),
	}
}

func snowflakeConfFromProto(c *agentdwhv1.SnowflakeConf) *SnowflakeConf {
	if c == nil {
		return nil
	}
	return &SnowflakeConf{
		Account:              c.GetAccount(),
		Warehouse:            c.GetWarehouse(),
		Role:                 c.GetRole(),
		Username:             c.GetUsername(),
		Password:             c.GetPassword(),
		PrivateKey:           c.GetPrivateKey(),
		PrivateKeyFile:       c.GetPrivateKeyFile(),
		PrivateKeyPassphrase: c.GetPrivateKeyPassphrase(),
		Databases:            c.GetDatabases(),
		UseGetDdl:            c.GetUseGetDdl(),
		AccountUsageDb:       c.GetAccountUsageDb(),
		AuthType:             c.GetAuthType(),
	}
}

func bigqueryConfFromProto(c *agentdwhv1.BigQueryConf) *BigQueryConf {
	if c == nil {
		return nil
	}
	return &BigQueryConf{
		ProjectId:             c.GetProjectId(),
		Region:                c.GetRegion(),
		ServiceAccountKey:     c.GetServiceAccountKey(),
		ServiceAccountKeyFile: c.GetServiceAccountKeyFile(),
		Datasets:              c.GetDatasets(),
	}
}

func redshiftConfFromProto(c *agentdwhv1.RedshiftConf) *RedshiftConf {
	if c == nil {
		return nil
	}
	return &RedshiftConf{
		Host:                   c.GetHost(),
		Port:                   int(c.GetPort()),
		Database:               c.GetDatabase(),
		Username:               c.GetUsername(),
		Password:               c.GetPassword(),
		FreshnessFromQueryLogs: c.GetFreshnessFromQueryLogs(),
	}
}

func mysqlConfFromProto(c *agentdwhv1.MySQLConf) *MySQLConf {
	if c == nil {
		return nil
	}
	return &MySQLConf{
		Host:          c.GetHost(),
		Port:          int(c.GetPort()),
		Database:      c.GetDatabase(),
		Username:      c.GetUsername(),
		Password:      c.GetPassword(),
		AllowInsecure: c.GetAllowInsecure(),
		Params:        c.GetParams(),
	}
}

func clickhouseConfFromProto(c *agentdwhv1.ClickhouseConf) *ClickhouseConf {
	if c == nil {
		return nil
	}
	return &ClickhouseConf{
		Host:          c.GetHost(),
		Port:          int(c.GetPort()),
		Database:      c.GetDatabase(),
		Username:      c.GetUsername(),
		Password:      c.GetPassword(),
		AllowInsecure: c.GetAllowInsecure(),
	}
}

func trinoConfFromProto(c *agentdwhv1.TrinoConf) *TrinoConf {
	if c == nil {
		return nil
	}
	return &TrinoConf{
		Host:                c.GetHost(),
		Port:                int(c.GetPort()),
		UsePlaintext:        c.GetUsePlaintext(),
		Username:            c.GetUsername(),
		Password:            c.GetPassword(),
		Catalogs:            c.GetCatalogs(),
		NoShowCreateView:    c.GetNoShowCreateView(),
		NoShowCreateTable:   c.GetNoShowCreateTable(),
		NoMaterializedViews: c.GetNoMaterializedViews(),
		FetchTableComments:  c.GetFetchTableComments(),
	}
}

func databricksConfFromProto(c *agentdwhv1.DatabricksConf) *DatabricksConf {
	if c == nil {
		return nil
	}
	return &DatabricksConf{
		WorkspaceUrl:               c.GetWorkspaceUrl(),
		AuthToken:                  c.GetAuthToken(),
		AuthClient:                 c.GetAuthClient(),
		AuthSecret:                 c.GetAuthSecret(),
		Warehouse:                  c.GetWarehouse(),
		RefreshTableMetrics:        c.GetRefreshTableMetrics(),
		RefreshTableMetricsUseScan: c.GetRefreshTableMetricsUseScan(),
		FetchTableTags:             c.GetFetchTableTags(),
		UseShowCreateTable:         c.GetUseShowCreateTable(),
	}
}

func mssqlConfFromProto(c *agentdwhv1.MSSQLConf) *MSSQLConf {
	if c == nil {
		return nil
	}
	return &MSSQLConf{
		Host:                c.GetHost(),
		Port:                int(c.GetPort()),
		Database:            c.GetDatabase(),
		Username:            c.GetUsername(),
		Password:            c.GetPassword(),
		TrustCert:           c.GetTrustCert(),
		Encrypt:             c.GetEncrypt(),
		FedAuth:             c.GetFedAuth(),
		AccessToken:         c.GetAccessToken(),
		ApplicationClientId: c.GetApplicationClientId(),
	}
}

func oracleConfFromProto(c *agentdwhv1.OracleConf) *OracleConf {
	if c == nil {
		return nil
	}
	return &OracleConf{
		Host:               c.GetHost(),
		Port:               int(c.GetPort()),
		ServiceName:        c.GetServiceName(),
		Username:           c.GetUsername(),
		Password:           c.GetPassword(),
		SSL:                c.GetSsl(),
		SSLVerify:          c.GetSslVerify(),
		WalletPath:         c.GetWalletPath(),
		UseDiagnosticsPack: c.GetUseDiagnosticsPack(),
	}
}

func duckdbConfFromProto(c *agentdwhv1.DuckDBConf) *DuckDBConf {
	if c == nil {
		return nil
	}
	return &DuckDBConf{
		Database:          c.GetDatabase(),
		MotherduckAccount: c.GetMotherduckAccount(),
		MotherduckToken:   c.GetMotherduckToken(),
	}
}
