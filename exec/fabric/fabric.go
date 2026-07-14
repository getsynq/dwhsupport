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
	"strings"

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
// leave the client credentials empty. On-prem agents running on a user's own
// machine can instead set AuthType to an ambient mode (see below) to reuse the
// host's Azure identity (az login / managed identity) with no stored secret.
type FabricConf struct {
	// Host is the workspace SQL analytics endpoint, e.g.
	// "<workspace-id>.datawarehouse.fabric.microsoft.com".
	Host string
	// Database is the default execution database — the context in which
	// *unqualified* ad-hoc/monitor SQL resolves (e.g. `FROM sales.products`).
	// Optional: defaults to "master" (the always-present workspace entry point),
	// which is sufficient because metadata scrapping and generated metrics SQL
	// are fully database-qualified via cross-database three-part names. Set it
	// only when unqualified queries should resolve against a specific database.
	//
	// Which databases to *scrape* is a separate, scrapper-level concern — see
	// FabricScrapperConf.Databases — so it is deliberately not on this
	// connection config (mirroring TrinoScrapperConf.Catalogs /
	// BigQueryScrapperConf.Datasets).
	Database string

	// AuthType selects the authentication method — one of the AuthType*
	// constants below, matched case-insensitively. Empty defaults to a service
	// principal (ClientID + ClientSecret), unless AccessToken is set (a
	// non-empty AccessToken always wins). The ambient modes
	// (AuthTypeAzureCLI/Default/ManagedIdentity) authenticate as the host's own
	// Azure identity with no stored credential and are intended for on-prem
	// agents. They are opt-in — engaged only when AuthType names one — so a
	// hosted caller that leaves AuthType empty with no credentials fails closed
	// rather than silently inheriting the host process identity.
	AuthType string

	// ClientID is the Entra application (client) ID of the service principal
	// (default auth), or the user-assigned identity client ID for
	// AuthTypeManagedIdentity.
	ClientID string
	// ClientSecret is the service principal's client secret.
	ClientSecret string
	// TenantID is the Entra tenant (directory) ID. Optional: when empty the
	// tenant is inferred from the Fabric endpoint. Provide it for tenants where
	// the server-supplied authority is not the SP's home tenant.
	TenantID string

	// AccessToken is a pre-acquired Entra OAuth access token for the SQL scope
	// (https://database.windows.net/.default). When set it takes precedence over
	// all other authentication methods — used by hosts that acquire the token
	// out of band (managed identity, workload identity federation, certificate).
	AccessToken string
}

// AuthType values for FabricConf.AuthType. Named after the Azure credential
// types Fabric users configure elsewhere, so the choice reads the same here as
// in Azure tooling.
const (
	// AuthTypeServicePrincipal authenticates with an Entra service principal
	// (ClientID + ClientSecret). This is the default when AuthType is empty.
	AuthTypeServicePrincipal = "service_principal"
	// AuthTypeAzureCLI reuses the host's interactive `az login` session — the
	// local-execution / developer-machine case.
	AuthTypeAzureCLI = "azure_cli"
	// AuthTypeDefault uses Azure's DefaultAzureCredential chain (managed
	// identity → environment → workload identity → az CLI).
	AuthTypeDefault = "default"
	// AuthTypeManagedIdentity authenticates with an Azure managed identity; set
	// ClientID for a user-assigned identity, leave empty for system-assigned.
	AuthTypeManagedIdentity = "managed_identity"
)

// ToMSSQLConf translates the simplified Fabric configuration into the MSSQL
// executor configuration, fixing the settings Fabric always requires: TLS
// encryption on and the standard TDS port. The authentication method is derived
// from AuthType (with a pre-acquired AccessToken always taking precedence).
func (c *FabricConf) ToMSSQLConf() *dwhexecmssql.MSSQLConf {
	// The workspace SQL endpoint is shared across all its databases; master is
	// always present and connectable, so it is the default entry point when no
	// explicit execution database is configured.
	database := c.Database
	if database == "" {
		database = "master"
	}
	conf := &dwhexecmssql.MSSQLConf{
		Host:     c.Host,
		Port:     1433,
		Database: database,
		// Fabric endpoints require encryption and present a valid public CA
		// certificate, so never disable verification.
		Encrypt: "true",
	}

	// A pre-acquired token wins regardless of AuthType.
	if c.AccessToken != "" {
		conf.AccessToken = c.AccessToken
		return conf
	}

	switch strings.ToLower(c.AuthType) {
	case AuthTypeAzureCLI:
		conf.FedAuth = "ActiveDirectoryAzCli"
	case AuthTypeDefault:
		conf.FedAuth = "ActiveDirectoryDefault"
	case AuthTypeManagedIdentity:
		conf.FedAuth = "ActiveDirectoryManagedIdentity"
		// A non-empty ClientID selects a user-assigned identity; the azuread
		// driver reads it from the "user id" parameter.
		conf.User = c.ClientID
	default: // "" or AuthTypeServicePrincipal
		conf.FedAuth = "ActiveDirectoryServicePrincipal"
		// The azuread driver reads the client id from "user id" and splits an
		// optional "@tenant" suffix; the client secret goes in the password.
		conf.User = c.ClientID
		if c.TenantID != "" {
			conf.User = c.ClientID + "@" + c.TenantID
		}
		conf.Password = c.ClientSecret
	}
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
