// Package fabric provides a Microsoft Fabric executor. Fabric Warehouses (and
// Lakehouse SQL analytics endpoints) speak T-SQL over TDS, so this is a thin,
// opinionated wrapper over the MSSQL executor: it exposes only the knobs a
// Fabric connection actually needs and hard-codes the rest (port 1433, TLS on,
// Entra service-principal federated auth) so callers never have to guess which
// MSSQL auth flag combination Fabric requires.
//
// Fabric rejects SQL logins entirely — the only headless identity is a
// Microsoft Entra ID service principal (app registration + secret). That maps
// onto the go-mssqldb azuread driver's ActiveDirectoryServicePrincipal workflow,
// where the username is the application (client) ID and the password is the
// client secret.
package fabric

import (
	"context"

	dwhexecmssql "github.com/getsynq/dwhsupport/exec/mssql"
	"github.com/getsynq/dwhsupport/exec/querier"
	"github.com/getsynq/dwhsupport/exec/stdsql"
)

// FabricConf configures a connection to a Microsoft Fabric Warehouse or Lakehouse
// SQL analytics endpoint.
//
// Authentication is via a Microsoft Entra ID service principal by default
// (ClientID + ClientSecret). For hosted deployments that mint their own token
// (managed identity / workload-identity federation), set AccessToken instead and
// leave the client credentials empty.
type FabricConf struct {
	// Host is the workspace SQL analytics endpoint, e.g.
	// "<workspace-id>.datawarehouse.fabric.microsoft.com".
	Host string
	// Database is the Fabric item (Warehouse or Lakehouse) name to connect to.
	Database string

	// ClientID is the Entra application (client) ID of the service principal.
	ClientID string
	// ClientSecret is the service principal's client secret.
	ClientSecret string
	// TenantID is the Entra tenant (directory) ID. Optional: when empty the
	// tenant is inferred from the Fabric endpoint. Provide it for tenants where
	// the server-supplied authority is not the SP's home tenant.
	TenantID string

	// AccessToken is a pre-acquired Entra OAuth access token for the SQL scope
	// (https://database.windows.net/.default). When set it takes precedence over
	// the service-principal fields — used by hosts that acquire the token out of
	// band (managed identity, workload identity federation, certificate flow).
	AccessToken string
}

// ToMSSQLConf translates the simplified Fabric configuration into the MSSQL
// executor configuration, fixing the settings Fabric always requires: TLS
// encryption on, standard TDS port, and Entra service-principal federated auth
// (unless a pre-acquired access token was supplied).
func (c *FabricConf) ToMSSQLConf() *dwhexecmssql.MSSQLConf {
	conf := &dwhexecmssql.MSSQLConf{
		Host:     c.Host,
		Port:     1433,
		Database: c.Database,
		// Fabric endpoints require encryption and present a valid public CA
		// certificate, so never disable verification.
		Encrypt: "true",
	}

	if c.AccessToken != "" {
		conf.AccessToken = c.AccessToken
		return conf
	}

	conf.FedAuth = "ActiveDirectoryServicePrincipal"
	// The azuread driver reads the client id from "user id" and splits an
	// optional "@tenant" suffix; the client secret goes in the password.
	conf.User = c.ClientID
	if c.TenantID != "" {
		conf.User = c.ClientID + "@" + c.TenantID
	}
	conf.Password = c.ClientSecret
	return conf
}

// FabricExecutor is a Fabric-flavoured MSSQL executor. It embeds the MSSQL
// executor so all query execution (SQL comment enrichment, query stats, row
// scanning) is shared, and exists as its own type so Fabric has a distinct
// surface from raw MSSQL.
type FabricExecutor struct {
	*dwhexecmssql.MSSQLExecutor
	conf *FabricConf
}

var _ stdsql.StdSqlExecutor = &FabricExecutor{}

func NewFabricExecutor(ctx context.Context, conf *FabricConf) (*FabricExecutor, error) {
	inner, err := dwhexecmssql.NewMSSQLExecutor(ctx, conf.ToMSSQLConf())
	if err != nil {
		return nil, err
	}
	return &FabricExecutor{MSSQLExecutor: inner, conf: conf}, nil
}

// Conf returns the Fabric configuration this executor was built from.
func (e *FabricExecutor) Conf() *FabricConf { return e.conf }

// NewQuerier builds a type-safe querier over the executor's connection. It
// mirrors mssql.NewQuerier but is provided here so the Fabric scrapper depends
// only on the fabric package.
func NewQuerier[T any](conn *FabricExecutor) querier.Querier[T] {
	return stdsql.NewQuerier[T](conn.GetDb())
}

// IsPermissionError reports whether err indicates the connection's identity
// lacks privileges. Fabric surfaces the same SQL Server permission messages as
// MSSQL, so it delegates to the single source of truth.
func IsPermissionError(err error) bool {
	return dwhexecmssql.IsPermissionError(err)
}
