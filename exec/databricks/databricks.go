package databricks

import (
	"context"
	"database/sql"
	"database/sql/driver"

	"github.com/databricks/databricks-sdk-go"
	servicesql "github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/databricks/databricks-sdk-go/useragent"
	_ "github.com/databricks/databricks-sql-go"
	dbsql "github.com/databricks/databricks-sql-go"
	"github.com/databricks/databricks-sql-go/auth/oauth/m2m"
	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

type Auth interface {
	Configure(config *databricks.Config)
}

var _ Auth = &TokenAuth{}
var _ Auth = &OAuthM2mAuth{}

type TokenAuth struct {
	Token string
}

func (t TokenAuth) Configure(config *databricks.Config) {
	config.Token = t.Token
}

type OAuthM2mAuth struct {
	ClientId     string
	ClientSecret string
}

func (o OAuthM2mAuth) Configure(config *databricks.Config) {
	config.ClientID = o.ClientId
	config.ClientSecret = o.ClientSecret
}

type DatabricksConf struct {
	WorkspaceUrl string
	Auth         Auth
	WarehouseId  string
}

type Executor interface {
	stdsql.StdSqlExecutor
}

var _ stdsql.StdSqlExecutor = &DatabricksExecutor{}

type DatabricksExecutor struct {
	conf      *DatabricksConf
	sqlClient *sqlx.DB
}

func (e *DatabricksExecutor) GetDb() *sqlx.DB {
	return e.sqlClient
}

func (e *DatabricksExecutor) Close() error {
	return e.sqlClient.Close()
}

func NewDatabricksExecutor(ctx context.Context, conf *DatabricksConf) (*DatabricksExecutor, error) {

	useragent.WithProduct("synq", "1.0.0")

	databricksConfig := &databricks.Config{
		Host: conf.WorkspaceUrl,
	}
	conf.Auth.Configure(databricksConfig)
	client, err := databricks.NewWorkspaceClient(databricksConfig)
	if err != nil {
		return nil, err
	}

	// Poor man ping
	_, err = client.DataSources.List(ctx)
	if err != nil {
		return nil, exec.NewAuthError(err)
	}

	warehouses, err := client.Warehouses.ListAll(ctx, servicesql.ListWarehousesRequest{})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list all warehouse")
	}
	if len(warehouses) == 0 {
		return nil, errors.New("no warehouse found")
	}
	odbcParams := warehouses[0].OdbcParams
	if conf.WarehouseId != "" {
		warehouseResp, err := client.Warehouses.GetById(ctx, conf.WarehouseId)
		if err != nil {
			return nil, errors.Wrap(err, "failed to list get warehouse by id")
		}
		odbcParams = warehouseResp.OdbcParams
	}

	var connector driver.Connector
	switch t := conf.Auth.(type) {
	case *TokenAuth:
		connector, err = dbsql.NewConnector(
			dbsql.WithAccessToken(t.Token),
			dbsql.WithHTTPPath(odbcParams.Path),
			dbsql.WithServerHostname(odbcParams.Hostname),
			dbsql.WithUserAgentEntry("synq/1.0.0"),
		)
	case *OAuthM2mAuth:
		authenticator := m2m.NewAuthenticator(t.ClientId, t.ClientSecret, odbcParams.Hostname)
		connector, err = dbsql.NewConnector(
			dbsql.WithAuthenticator(authenticator),
			dbsql.WithHTTPPath(odbcParams.Path),
			dbsql.WithServerHostname(odbcParams.Hostname),
			dbsql.WithUserAgentEntry("synq/1.0.0"),
		)
	default:
		return nil, errors.Errorf("unsupported auth type %T", conf.Auth)
	}
	if err != nil {
		return nil, errors.Wrap(err, "failed to create databricks sql connector")
	}
	db := sql.OpenDB(connector)

	sqlClient := sqlx.NewDb(db, "databricks")

	return &DatabricksExecutor{sqlClient: sqlClient}, nil
}

func (e *DatabricksExecutor) QueryRows(ctx context.Context, sql string, args ...interface{}) (*sqlx.Rows, error) {
	return e.sqlClient.QueryxContext(ctx, sql, args...)
}
