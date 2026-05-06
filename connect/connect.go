package connect

import (
	"context"

	agentdwhv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/agent/dwh/v1"
	commonv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/common/v1"
	dwhexecathena "github.com/getsynq/dwhsupport/exec/athena"
	dwhexecbigquery "github.com/getsynq/dwhsupport/exec/bigquery"
	dwhexecclickhouse "github.com/getsynq/dwhsupport/exec/clickhouse"
	dwhexecdatabricks "github.com/getsynq/dwhsupport/exec/databricks"
	dwhexecmssql "github.com/getsynq/dwhsupport/exec/mssql"
	dwhexecmysql "github.com/getsynq/dwhsupport/exec/mysql"
	dwhexecoracle "github.com/getsynq/dwhsupport/exec/oracle"
	dwhexecpostgres "github.com/getsynq/dwhsupport/exec/postgres"
	dwhexecredshift "github.com/getsynq/dwhsupport/exec/redshift"
	dwhexecsnowflake "github.com/getsynq/dwhsupport/exec/snowflake"
	dwhexectrino "github.com/getsynq/dwhsupport/exec/trino"
	"github.com/getsynq/dwhsupport/scrapper"
	scrapperathena "github.com/getsynq/dwhsupport/scrapper/athena"
	scrapperbigquery "github.com/getsynq/dwhsupport/scrapper/bigquery"
	scrapperclickhouse "github.com/getsynq/dwhsupport/scrapper/clickhouse"
	scrapperdatabricks "github.com/getsynq/dwhsupport/scrapper/databricks"
	scrappermssql "github.com/getsynq/dwhsupport/scrapper/mssql"
	scrappermysql "github.com/getsynq/dwhsupport/scrapper/mysql"
	scrapperoracle "github.com/getsynq/dwhsupport/scrapper/oracle"
	scrapperpostgres "github.com/getsynq/dwhsupport/scrapper/postgres"
	scrapperredshift "github.com/getsynq/dwhsupport/scrapper/redshift"
	scrappersnowflake "github.com/getsynq/dwhsupport/scrapper/snowflake"
	scrappertrino "github.com/getsynq/dwhsupport/scrapper/trino"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/pkg/errors"
	"github.com/samber/lo"
)

func BigQuery(ctx context.Context, t *agentdwhv1.BigQueryConf) (*scrapperbigquery.BigQueryScrapper, error) {
	return scrapperbigquery.NewBigQueryScrapper(ctx, &scrapperbigquery.BigQueryScrapperConf{
		BigQueryConf: dwhexecbigquery.BigQueryConf{
			ProjectId:       t.GetProjectId(),
			Region:          t.GetRegion(),
			CredentialsJson: t.GetServiceAccountKey(),
			CredentialsFile: t.GetServiceAccountKeyFile(),
		},
		Datasets: t.GetDatasets(),
	})
}

func Clickhouse(ctx context.Context, t *agentdwhv1.ClickhouseConf) (*scrapperclickhouse.ClickhouseScrapper, error) {
	return scrapperclickhouse.NewClickhouseScrapper(ctx, scrapperclickhouse.ClickhouseScrapperConf{
		ClickhouseConf: dwhexecclickhouse.ClickhouseConf{
			Hostname:        t.GetHost(),
			Port:            int(t.GetPort()),
			Username:        t.GetUsername(),
			Password:        t.GetPassword(),
			DefaultDatabase: t.GetDatabase(),
			NoSsl:           t.GetAllowInsecure(),
		},
	})
}

func Databricks(ctx context.Context, t *agentdwhv1.DatabricksConf) (*scrapperdatabricks.DatabricksScrapper, error) {
	var auth dwhexecdatabricks.Auth
	if t.AuthToken != nil {
		auth = dwhexecdatabricks.NewTokenAuth(t.GetAuthToken())
	} else {
		auth = dwhexecdatabricks.NewOAuthM2mAuth(t.GetAuthClient(), t.GetAuthSecret())
	}

	return scrapperdatabricks.NewDatabricksScrapper(ctx, &scrapperdatabricks.DatabricksScrapperConf{
		DatabricksConf: dwhexecdatabricks.DatabricksConf{
			WorkspaceUrl: t.GetWorkspaceUrl(),
			Auth:         auth,
			WarehouseId:  t.GetWarehouse(),
		},
		RefreshTableMetrics:        t.GetRefreshTableMetrics(),
		RefreshTableMetricsUseScan: t.GetRefreshTableMetricsUseScan(),
		FetchTableTags:             t.GetFetchTableTags(),
		UseShowCreateTable:         t.GetUseShowCreateTable(),
	})
}

func MySQL(ctx context.Context, t *agentdwhv1.MySQLConf) (*scrappermysql.MySQLScrapper, error) {
	return scrappermysql.NewMySQLScrapper(ctx, &scrappermysql.MySQLScrapperConf{
		MySQLConf: dwhexecmysql.MySQLConf{
			User:          t.GetUsername(),
			Password:      t.GetPassword(),
			Host:          t.GetHost(),
			Port:          int(t.GetPort()),
			Database:      t.GetDatabase(),
			AllowInsecure: t.GetAllowInsecure(),
			Params:        t.GetParams(),
		},
	})
}

func Postgres(ctx context.Context, t *agentdwhv1.PostgresConf) (*scrapperpostgres.PostgresScrapper, error) {
	return scrapperpostgres.NewPostgresScrapper(ctx, &scrapperpostgres.PostgresScapperConf{
		PostgresConf: dwhexecpostgres.PostgresConf{
			User:          t.GetUsername(),
			Password:      t.GetPassword(),
			Database:      t.GetDatabase(),
			Host:          t.GetHost(),
			Port:          int(t.GetPort()),
			AllowInsecure: t.GetAllowInsecure(),
		},
	})
}

func Redshift(ctx context.Context, t *agentdwhv1.RedshiftConf) (*scrapperredshift.RedshiftScrapper, error) {
	return scrapperredshift.NewRedshiftScrapper(ctx, &scrapperredshift.RedshiftScrapperConf{
		RedshiftConf: dwhexecredshift.RedshiftConf{
			User:     t.GetUsername(),
			Password: t.GetPassword(),
			Database: t.GetDatabase(),
			Host:     t.GetHost(),
			Port:     int(t.GetPort()),
		},
		FreshnessFromQueryLogs: t.GetFreshnessFromQueryLogs(),
	})
}

func Snowflake(ctx context.Context, t *agentdwhv1.SnowflakeConf) (*scrappersnowflake.SnowflakeScrapper, error) {
	return scrappersnowflake.NewSnowflakeScrapper(ctx, &scrappersnowflake.SnowflakeScrapperConf{
		SnowflakeConf: dwhexecsnowflake.SnowflakeConf{
			User:                 t.GetUsername(),
			Password:             t.GetPassword(),
			PrivateKey:           []byte(t.GetPrivateKey()),
			PrivateKeyFile:       t.GetPrivateKeyFile(),
			PrivateKeyPassphrase: t.GetPrivateKeyPassphrase(),
			Account:              t.GetAccount(),
			Warehouse:            t.GetWarehouse(),
			Databases:            t.GetDatabases(),
			Role:                 t.GetRole(),
			AuthType:             t.GetAuthType(),
		},
		NoGetDll:       !t.GetUseGetDdl(),
		AccountUsageDb: lo.EmptyableToPtr(t.GetAccountUsageDb()),
	})
}

func Trino(ctx context.Context, trino *agentdwhv1.TrinoConf) (scrapper.Scrapper, error) {
	return scrappertrino.NewTrinoScrapper(ctx, &scrappertrino.TrinoScrapperConf{
		TrinoConf: &dwhexectrino.TrinoConf{
			Host:      trino.GetHost(),
			Port:      int(trino.GetPort()),
			Plaintext: trino.GetUsePlaintext(),
			User:      trino.GetUsername(),
			Password:  trino.GetPassword(),
			Source:    "SYNQ",
		},
		Catalogs:               trino.GetCatalogs(),
		UseShowCreateView:      !trino.GetNoShowCreateView(),
		UseShowCreateTable:     !trino.GetNoShowCreateTable(),
		FetchMaterializedViews: !trino.GetNoMaterializedViews(),
		FetchTableComments:     trino.GetFetchTableComments(),
	})
}

func MSSQL(ctx context.Context, t *agentdwhv1.MSSQLConf) (*scrappermssql.MSSQLScrapper, error) {
	return scrappermssql.NewMSSQLScrapper(ctx, &scrappermssql.MSSQLScrapperConf{
		MSSQLConf: dwhexecmssql.MSSQLConf{
			User:                t.GetUsername(),
			Password:            t.GetPassword(),
			Host:                t.GetHost(),
			Port:                int(t.GetPort()),
			Database:            t.GetDatabase(),
			TrustCert:           t.GetTrustCert(),
			Encrypt:             t.GetEncrypt(),
			FedAuth:             t.GetFedAuth(),
			AccessToken:         t.GetAccessToken(),
			ApplicationClientID: t.GetApplicationClientId(),
		},
	})
}

func Oracle(ctx context.Context, t *agentdwhv1.OracleConf) (*scrapperoracle.OracleScrapper, error) {
	return scrapperoracle.NewOracleScrapper(ctx, &scrapperoracle.OracleScrapperConf{
		OracleConf: dwhexecoracle.OracleConf{
			User:        t.GetUsername(),
			Password:    t.GetPassword(),
			Host:        t.GetHost(),
			Port:        int(t.GetPort()),
			ServiceName: t.GetServiceName(),
			SSL:         t.GetSsl(),
			SSLVerify:   t.GetSslVerify(),
			WalletPath:  t.GetWalletPath(),
		},
		UseDiagnosticsPack: t.GetUseDiagnosticsPack(),
	})
}

// duckDBConnector is set by connect_duckdb.go when building with CGO enabled.
// DuckDB requires CGO due to its native C library dependency.
var duckDBConnector func(ctx context.Context, conf *agentdwhv1.DuckDBConf) (scrapper.Scrapper, error)

func DuckDB(ctx context.Context, conf *agentdwhv1.DuckDBConf) (scrapper.Scrapper, error) {
	if duckDBConnector == nil {
		return nil, errors.New("duckdb support not available (requires CGO-enabled build)")
	}
	return duckDBConnector(ctx, conf)
}

func Connect(ctx context.Context, conf *agentdwhv1.Connection) (scrapper.Scrapper, error) {
	switch t := conf.Config.(type) {
	case *agentdwhv1.Connection_Bigquery:
		return BigQuery(ctx, t.Bigquery)
	case *agentdwhv1.Connection_Clickhouse:
		return Clickhouse(ctx, t.Clickhouse)
	case *agentdwhv1.Connection_Databricks:
		return Databricks(ctx, t.Databricks)
	case *agentdwhv1.Connection_Mysql:
		return MySQL(ctx, t.Mysql)
	case *agentdwhv1.Connection_Postgres:
		return Postgres(ctx, t.Postgres)
	case *agentdwhv1.Connection_Redshift:
		return Redshift(ctx, t.Redshift)
	case *agentdwhv1.Connection_Snowflake:
		return Snowflake(ctx, t.Snowflake)
	case *agentdwhv1.Connection_Trino:
		return Trino(ctx, t.Trino)
	case *agentdwhv1.Connection_Mssql:
		return MSSQL(ctx, t.Mssql)
	case *agentdwhv1.Connection_Oracle:
		return Oracle(ctx, t.Oracle)
	case *agentdwhv1.Connection_Duckdb:
		return DuckDB(ctx, t.Duckdb)
	case *agentdwhv1.Connection_Athena:
		return Athena(ctx, t.Athena)
	default:
		return nil, errors.New("unsupported database type")
	}
}

// Athena builds an AthenaScrapper for the on-prem agent. AllowDefaultChain is
// true here so the agent host's instance role / env vars / shared config are
// usable when the customer leaves explicit auth fields empty. The cloud-side
// translator MUST construct AthenaConf without AllowDefaultChain so a
// misconfigured customer integration cannot inherit SYNQ's AWS identity.
func Athena(ctx context.Context, t *agentdwhv1.AthenaConf) (*scrapperathena.AthenaScrapper, error) {
	return scrapperathena.NewAthenaScrapper(ctx, &scrapperathena.AthenaScrapperConf{
		AthenaConf: &dwhexecathena.AthenaConf{
			Region:            t.GetRegion(),
			Workgroup:         t.GetWorkgroup(),
			Catalog:           t.GetCatalog(),
			AccessKeyID:       t.GetAccessKeyId(),
			SecretAccessKey:   t.GetSecretAccessKey(),
			SessionToken:      t.GetSessionToken(),
			AwsProfile:        t.GetAwsProfile(),
			RoleArn:           t.GetRoleArn(),
			ExternalID:        t.GetExternalId(),
			RoleSessionName:   t.GetRoleSessionName(),
			AllowDefaultChain: true,
		},
		UseShowCreateView:  t.GetUseShowCreateView(),
		UseShowCreateTable: t.GetUseShowCreateTable(),
		Scope:              athenaScopeFromProto(t.GetScope()),
	})
}

// athenaScopeFromProto inlines the buf-generated ScopeFilter → runtime
// scope.ScopeFilter conversion. Kept here (not in scrapper/scope) on purpose:
// the same proto file is also generated under github.com/getsynq/api/common/v1
// in the cloud workspace, and any binary that links BOTH Go packages panics on
// init because protobuf's global registry rejects duplicate registrations of
// the same .proto file path. By only importing the buf flavor in this package
// (which is exclusive to agent callers anyway), the runtime scope.ScopeFilter
// type stays free of any proto generated package and is safe to import from
// cloud handlers that already use the workspace flavor.
func athenaScopeFromProto(p *commonv1.ScopeFilter) *scope.ScopeFilter {
	if p == nil {
		return nil
	}
	if len(p.GetInclude()) == 0 && len(p.GetExclude()) == 0 {
		return nil
	}
	out := &scope.ScopeFilter{
		Include: make([]scope.ScopeRule, 0, len(p.GetInclude())),
		Exclude: make([]scope.ScopeRule, 0, len(p.GetExclude())),
	}
	for _, r := range p.GetInclude() {
		out.Include = append(out.Include, scope.ScopeRule{
			Database: r.GetDatabase(),
			Schema:   r.GetSchema(),
			Table:    r.GetTable(),
		})
	}
	for _, r := range p.GetExclude() {
		out.Exclude = append(out.Exclude, scope.ScopeRule{
			Database: r.GetDatabase(),
			Schema:   r.GetSchema(),
			Table:    r.GetTable(),
		})
	}
	return out
}
