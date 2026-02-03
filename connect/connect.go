package connect

import (
	"context"

	agentdwhv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/agent/dwh/v1"
	dwhexecbigquery "github.com/getsynq/dwhsupport/exec/bigquery"
	dwhexecclickhouse "github.com/getsynq/dwhsupport/exec/clickhouse"
	dwhexecdatabricks "github.com/getsynq/dwhsupport/exec/databricks"
	dwhexecredshift "github.com/getsynq/dwhsupport/exec/redshift"
	dwhexecsnowflake "github.com/getsynq/dwhsupport/exec/snowflake"
	dwhexectrino "github.com/getsynq/dwhsupport/exec/trino"
	"github.com/getsynq/dwhsupport/scrapper"
	scrapperbigquery "github.com/getsynq/dwhsupport/scrapper/bigquery"
	scrapperclickhouse "github.com/getsynq/dwhsupport/scrapper/clickhouse"
	scrapperdatabricks "github.com/getsynq/dwhsupport/scrapper/databricks"
	scrappermysql "github.com/getsynq/dwhsupport/scrapper/mysql"
	scrapperpostgres "github.com/getsynq/dwhsupport/scrapper/postgres"
	scrapperredshift "github.com/getsynq/dwhsupport/scrapper/redshift"
	scrappersnowflake "github.com/getsynq/dwhsupport/scrapper/snowflake"
	scrappertrino "github.com/getsynq/dwhsupport/scrapper/trino"
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
		User:          t.GetUsername(),
		Password:      t.GetPassword(),
		Host:          t.GetHost(),
		Port:          int(t.GetPort()),
		Database:      t.GetDatabase(),
		AllowInsecure: t.GetAllowInsecure(),
		Params:        t.GetParams(),
	})
}

func Postgres(ctx context.Context, t *agentdwhv1.PostgresConf) (*scrapperpostgres.PostgresScrapper, error) {
	return scrapperpostgres.NewPostgresScrapper(ctx, &scrapperpostgres.PostgresScapperConf{
		User:          t.GetUsername(),
		Password:      t.GetPassword(),
		Database:      t.GetDatabase(),
		Host:          t.GetHost(),
		Port:          int(t.GetPort()),
		AllowInsecure: t.GetAllowInsecure(),
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
	default:
		return nil, errors.New("unsupported database type")
	}
}
