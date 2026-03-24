package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"

	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/querycontext"
	"github.com/getsynq/dwhsupport/exec/querystats"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/jmoiron/sqlx"

	mssqldb "github.com/microsoft/go-mssqldb"
	// Register Azure AD authentication driver alongside the standard "sqlserver" driver.
	_ "github.com/microsoft/go-mssqldb/azuread"
)

// MSSQLConf configures a connection to Microsoft SQL Server or Azure SQL Database.
//
// Authentication methods (in order of priority):
//  1. Access Token: set AccessToken to a pre-acquired Azure AD OAuth token.
//  2. Azure AD Federated Auth: set FedAuth to an Azure AD method (e.g. ActiveDirectoryDefault,
//     ActiveDirectoryMSI, ActiveDirectoryServicePrincipal). Used with Azure SQL Database.
//  3. SQL Server Authentication: set User and Password for standard username/password auth.
//
// For Azure SQL Database, Azure AD authentication (options 1-2) is recommended.
// For on-premises SQL Server, use SQL Server Authentication (option 3).
type MSSQLConf struct {
	// User is the SQL Server login or Azure AD username.
	// For Azure AD Service Principal, this is the Application (Client) ID.
	// Leave empty when using AccessToken or Azure AD Managed Identity.
	User string
	// Password is the SQL Server login password or Azure AD client secret.
	Password string
	// Host is the hostname or IP address.
	// For Azure SQL: e.g. "yourserver.database.windows.net"
	Host string
	// Port is the SQL Server port. Default: 1433.
	Port int
	// Database is the database name to connect to.
	// SQL Server is database-scoped — create a separate connection per database.
	Database string

	// TrustCert skips TLS server certificate verification.
	// Not recommended for production Azure SQL connections.
	TrustCert bool
	// Encrypt controls connection encryption.
	// Values: "true" (default, required for Azure SQL), "false", "disable".
	Encrypt string

	// FedAuth specifies the Azure AD federated authentication method.
	// When set, the azuread driver is used instead of the standard sqlserver driver.
	// Common values for cloud deployments:
	//   - "ActiveDirectoryDefault": chained credential (Managed Identity → environment → CLI)
	//   - "ActiveDirectoryMSI": Azure Managed Identity (system or user-assigned)
	//   - "ActiveDirectoryServicePrincipal": app registration with client ID + secret
	//   - "ActiveDirectoryAzCli": reuse Azure CLI authentication
	// See https://pkg.go.dev/github.com/microsoft/go-mssqldb/azuread for all options.
	FedAuth string

	// AccessToken is a pre-acquired Azure AD OAuth access token.
	// When set, it takes precedence over all other authentication methods.
	// The token is passed directly to the driver via NewAccessTokenConnector.
	AccessToken string

	// ApplicationClientID is the Azure AD Application (Client) ID.
	// Used with ActiveDirectoryServicePrincipal (identifies the app registration)
	// and ActiveDirectoryMSI with user-assigned managed identity (resource ID).
	ApplicationClientID string
}

var _ stdsql.StdSqlExecutor = &MSSQLExecutor{}

type MSSQLExecutor struct {
	conf *MSSQLConf
	db   *sqlx.DB
}

func (e *MSSQLExecutor) GetDb() *sqlx.DB {
	return e.db
}

func NewMSSQLExecutor(ctx context.Context, conf *MSSQLConf) (*MSSQLExecutor, error) {
	if conf.Port == 0 {
		conf.Port = 1433
	}

	var db *sqlx.DB

	switch {
	case conf.AccessToken != "":
		// Token-based auth: use the access token connector directly.
		connector, err := mssqldb.NewAccessTokenConnector(
			buildConnectionString(conf),
			func() (string, error) { return conf.AccessToken, nil },
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create access token connector: %w", err)
		}
		db = sqlx.NewDb(sql.OpenDB(connector), "sqlserver")

	case conf.FedAuth != "":
		// Azure AD federated auth: use the azuread driver.
		connStr := buildConnectionString(conf)
		var err error
		db, err = sqlx.Open("azuresql", connStr)
		if err != nil {
			return nil, err
		}

	default:
		// Standard SQL Server authentication: username/password.
		connStr := buildConnectionString(conf)
		var err error
		db, err = sqlx.Open("sqlserver", connStr)
		if err != nil {
			return nil, err
		}
	}

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, exec.NewAuthError(err)
	}

	return &MSSQLExecutor{conf: conf, db: db}, nil
}

func buildConnectionString(conf *MSSQLConf) string {
	query := url.Values{}
	query.Add("database", conf.Database)
	query.Add("app name", "synq.io")
	if conf.TrustCert {
		query.Add("TrustServerCertificate", "true")
	}
	if conf.Encrypt != "" {
		query.Add("encrypt", conf.Encrypt)
	}
	if conf.FedAuth != "" {
		query.Add("fedauth", conf.FedAuth)
	}
	if conf.ApplicationClientID != "" {
		query.Add("applicationclientid", conf.ApplicationClientID)
	}

	u := &url.URL{
		Scheme:   "sqlserver",
		Host:     fmt.Sprintf("%s:%d", conf.Host, conf.Port),
		RawQuery: query.Encode(),
	}

	// Only set user info for password-based auth.
	if conf.AccessToken == "" && conf.User != "" {
		u.User = url.UserPassword(conf.User, conf.Password)
	}

	return u.String()
}

func (e *MSSQLExecutor) QueryRows(ctx context.Context, sql string, args ...interface{}) (*sqlx.Rows, error) {
	sql = querycontext.AppendSQLComment(ctx, sql)
	return e.db.QueryxContext(ctx, sql, args...)
}

func (e *MSSQLExecutor) Select(ctx context.Context, dest any, query string, args ...any) error {
	query = querycontext.AppendSQLComment(ctx, query)
	collector, ctx := querystats.Start(ctx)
	defer collector.Finish()
	return e.db.SelectContext(ctx, dest, query, args...)
}

func (e *MSSQLExecutor) Exec(ctx context.Context, query string, args ...any) error {
	query = querycontext.AppendSQLComment(ctx, query)
	collector, ctx := querystats.Start(ctx)
	defer collector.Finish()
	_, err := e.db.ExecContext(ctx, query, args...)
	return err
}

func (e *MSSQLExecutor) Close() error {
	return e.db.Close()
}
