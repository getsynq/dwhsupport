package fabric

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	dwhexecfabric "github.com/getsynq/dwhsupport/exec/fabric"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
)

// platformUsersGrant is what lets the connection's identity list every user of
// the workspace.
const platformUsersGrant = "the Member or Admin role in the Fabric workspace for the connection's identity " +
	"(workspace Manage access), and for a service principal the tenant setting 'Service principals can use Fabric APIs'"

// Principal types of a workspace role assignment.
const (
	principalTypeUser             = "User"
	principalTypeGroup            = "Group"
	principalTypeServicePrincipal = "ServicePrincipal"
)

// databaseUsersSql lists the identities added to the connected database:
// Entra users, service principals and groups (authentication_type 4), and
// the rare contained ones. dbo and guest are not identities. Only the
// connection's own row can be mapped to the login name query history
// reports, through SUSER_SNAME().
const databaseUsersSql = `SELECT
	dp.name                           AS name,
	CONVERT(varchar(200), dp.sid, 1)  AS sid,
	dp.type_desc                      AS type,
	CASE WHEN dp.sid = SUSER_SID() THEN SUSER_SNAME() END AS own_login
FROM sys.database_principals dp
WHERE dp.type IN ('S', 'U', 'G', 'E', 'X')
  AND dp.authentication_type IN (2, 3, 4)
  AND dp.sid IS NOT NULL`

// databaseRolesSql lists the database roles held by those identities,
// prefixed with the database's name: an identity holds different roles in
// each database of the workspace.
const databaseRolesSql = `SELECT
	CONVERT(varchar(200), u.sid, 1) AS sid,
	DB_NAME() + '.' + r.name        AS role
FROM sys.database_role_members drm
JOIN sys.database_principals r ON r.principal_id = drm.role_principal_id
JOIN sys.database_principals u ON u.principal_id = drm.member_principal_id
WHERE u.sid IS NOT NULL`

type databaseUserRow struct {
	Name     string         `db:"name"`
	Sid      string         `db:"sid"`
	Type     sql.NullString `db:"type"`
	OwnLogin sql.NullString `db:"own_login"`
}

type databaseRoleRow struct {
	Sid  string `db:"sid"`
	Role string `db:"role"`
}

// databaseUsers is what the connected database states about its identities.
type databaseUsers struct {
	Users []*databaseUserRow
	// RolesBySid holds each identity's database roles, keyed by the GUID its
	// SID encodes.
	RolesBySid map[string][]string
	// RolesErr is why the roles could not be read, nil when they could.
	RolesErr error
}

// Sources of QueryPlatformUsers, the workspace API first: it names every
// member of the workspace by the login query history reports, while the
// database knows only the identities added to it.
const (
	sourceWorkspaceRoleAssignments = "fabric.workspace_role_assignments"
	sourceDatabasePrincipals       = "fabric.sys.database_principals"
)

// QueryPlatformUsers reads two sources, each reported on its own:
//
//   - fabric.workspace_role_assignments (Fabric REST API): Entra users by user
//     principal name and service principals as <application id>@<tenant id>,
//     which is how query history (queryinsights login_name) names them, with
//     their workspace role. It needs the Member or Admin workspace role, and a
//     credential that can call the API (not a pre-acquired SQL token).
//   - fabric.sys.database_principals: the identities added to the connected
//     database, with their database roles. A user of it carries the login the
//     API source gives the same identity (matched by the GUID its SID
//     encodes), so the two reconcile; without the API only the connection's
//     own login is known, the others keep their database name.
//
// A source that could not be read is reported refused when a grant would let
// it answer, unavailable when the connection's configuration cannot reach it
// (a pre-acquired SQL token, a host that names no workspace, a credential that
// cannot be built) or the SQL surface lacks the view, and failed otherwise (a
// 5xx, a timeout, a dropped connection).
func (e *FabricScrapper) QueryPlatformUsers(ctx context.Context) (*scrapper.PlatformUsers, error) {
	identity, err := dwhexecfabric.ParseHostIdentity(e.conf.Host)
	tenantID := e.conf.TenantID
	var assignments []*dwhexecfabric.WorkspaceRoleAssignment
	var apiErr error
	if err != nil {
		apiErr = &apiUnavailableError{err: errors.Wrap(err, "the host names no Fabric workspace")}
	} else {
		if tenantID == "" {
			tenantID = identity.TenantID
		}
		assignments, apiErr = e.listRoleAssignments(ctx, identity.WorkspaceID)
	}

	db, dbErr := e.queryDatabaseUsers(ctx)
	return buildPlatformUsers(ctx, assignments, apiErr, db, dbErr, tenantID, e.databaseName())
}

// apiUnavailableError is why the connection's configuration cannot call the
// Fabric API at all. No grant fixes it, so the API source is unavailable, not
// refused.
type apiUnavailableError struct{ err error }

func (e *apiUnavailableError) Error() string { return e.err.Error() }
func (e *apiUnavailableError) Unwrap() error { return e.err }

func isAPIUnavailable(err error) bool {
	var unavailable *apiUnavailableError
	return errors.As(err, &unavailable) || errors.Is(err, dwhexecfabric.ErrNoAPICredential)
}

func (e *FabricScrapper) listRoleAssignments(ctx context.Context, workspaceID string) ([]*dwhexecfabric.WorkspaceRoleAssignment, error) {
	client, err := dwhexecfabric.NewAPIClient(&e.conf.FabricConf)
	if err != nil {
		return nil, &apiUnavailableError{err: err}
	}
	return client.ListWorkspaceRoleAssignments(ctx, workspaceID)
}

func (e *FabricScrapper) databaseName() string {
	return e.conf.ToMSSQLConf().Database
}

func (e *FabricScrapper) queryDatabaseUsers(ctx context.Context) (*databaseUsers, error) {
	users, err := selectRows[databaseUserRow](ctx, e.executor, databaseUsersSql)
	if err != nil {
		return nil, err
	}
	result := &databaseUsers{Users: users, RolesBySid: map[string][]string{}}
	roles, err := selectRows[databaseRoleRow](ctx, e.executor, databaseRolesSql)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		result.RolesErr = err
		return result, nil
	}
	for _, r := range roles {
		if guid, ok := sidGUID(r.Sid); ok {
			result.RolesBySid[guid] = append(result.RolesBySid[guid], r.Role)
		}
	}
	return result, nil
}

// buildPlatformUsers reports each source on its own; one that could not be
// read is refused, unavailable or failed as its error says.
func buildPlatformUsers(
	ctx context.Context,
	assignments []*dwhexecfabric.WorkspaceRoleAssignment,
	apiErr error,
	db *databaseUsers,
	dbErr error,
	tenantID string,
	databaseName string,
) (*scrapper.PlatformUsers, error) {
	var apiSource, dbSource *scrapper.PlatformUserListing
	loginsByGUID := map[string]string{}
	if apiErr != nil {
		apiSource = scrapper.PlatformUserSourceError(sourceWorkspaceRoleAssignments, scrapper.PlatformUserSourceAPI, apiErr,
			dwhexecfabric.IsPermissionError, isAPIUnavailable)
	} else {
		apiSource = platformUsersFromAssignments(assignments, tenantID)
		loginsByGUID = loginsByIdentityGUID(assignments, tenantID)
	}
	if dbErr != nil {
		dbSource = scrapper.PlatformUserSourceError(sourceDatabasePrincipals, scrapper.PlatformUserSourceSQL, dbErr,
			dwhexecfabric.IsPermissionError, dwhexecfabric.IsUnavailableError)
	} else {
		dbSource = platformUsersFromDatabase(db, loginsByGUID, databaseName)
	}
	return scrapper.CollectPlatformUsers(ctx, apiSource, dbSource)
}

func platformUsersFromAssignments(assignments []*dwhexecfabric.WorkspaceRoleAssignment, tenantID string) *scrapper.PlatformUserListing {
	result := &scrapper.PlatformUserListing{
		Source:       sourceWorkspaceRoleAssignments,
		Kind:         scrapper.PlatformUserSourceAPI,
		Completeness: scrapper.PlatformUsersComplete,
	}
	for _, f := range []scrapper.PlatformUserFact{
		scrapper.PlatformUserFactComment,
		scrapper.PlatformUserFactDisabled,
		scrapper.PlatformUserFactCreatedAt,
		scrapper.PlatformUserFactLastLoginAt,
		scrapper.PlatformUserFactDefaultRole,
	} {
		result.Skip(f, "the Fabric API does not state it for a workspace member")
	}
	result.Skip(scrapper.PlatformUserFactEmail, "the Fabric API states a user principal name, which is the login, not an email")

	var notListed []string
	for _, a := range assignments {
		u := userFromPrincipal(a.Principal, tenantID)
		if u == nil {
			notListed = append(notListed, fmt.Sprintf("%s %q", a.Principal.Type, a.Principal.DisplayName))
			continue
		}
		if a.Role != "" {
			u.Roles = []string{a.Role}
		}
		result.Users = append(result.Users, u)
	}
	if len(notListed) > 0 {
		sort.Strings(notListed)
		result.Completeness = scrapper.PlatformUsersLimited
		result.CompletenessReason = "workspace roles held through " + strings.Join(notListed, ", ") +
			" reach identities that are not listed: the members of a group sign in as themselves"
	}
	return result
}

// userFromPrincipal turns a role holder into a user, or nil for one that is
// not a login (a group, a service principal profile).
func userFromPrincipal(p dwhexecfabric.WorkspacePrincipal, tenantID string) *scrapper.PlatformUser {
	u := &scrapper.PlatformUser{PlatformId: p.ID, Type: p.Type, DisplayName: p.DisplayName}
	switch p.Type {
	case principalTypeUser:
		if p.UserDetails == nil || p.UserDetails.UserPrincipalName == "" {
			return nil
		}
		u.Login = p.UserDetails.UserPrincipalName
	case principalTypeServicePrincipal:
		if p.ServicePrincipalDetails == nil || p.ServicePrincipalDetails.AadAppID == "" {
			return nil
		}
		u.Login = servicePrincipalLogin(p.ServicePrincipalDetails.AadAppID, tenantID)
	default:
		return nil
	}
	return u
}

// servicePrincipalLogin is the login name Fabric gives a service principal:
// its application id and tenant id joined by @, lower-cased GUIDs.
func servicePrincipalLogin(appID, tenantID string) string {
	login := strings.ToLower(appID)
	if tenantID != "" {
		login += "@" + strings.ToLower(tenantID)
	}
	return login
}

// loginsByIdentityGUID maps the GUID a database user's SID encodes to the
// login the API gives the same identity: a user's SID is its Entra object id,
// a service principal's its application id.
func loginsByIdentityGUID(assignments []*dwhexecfabric.WorkspaceRoleAssignment, tenantID string) map[string]string {
	logins := map[string]string{}
	for _, a := range assignments {
		u := userFromPrincipal(a.Principal, tenantID)
		if u == nil {
			continue
		}
		switch a.Principal.Type {
		case principalTypeUser:
			logins[strings.ToLower(a.Principal.ID)] = u.Login
		case principalTypeServicePrincipal:
			logins[strings.ToLower(a.Principal.ServicePrincipalDetails.AadAppID)] = u.Login
		}
	}
	return logins
}

func platformUsersFromDatabase(db *databaseUsers, loginsByGUID map[string]string, databaseName string) *scrapper.PlatformUserListing {
	result := &scrapper.PlatformUserListing{
		Source:       sourceDatabasePrincipals,
		Kind:         scrapper.PlatformUserSourceSQL,
		Completeness: scrapper.PlatformUsersLimited,
		CompletenessReason: "database " + databaseName + " holds only the identities added to it explicitly; " +
			"workspace members reach it through their workspace role without being listed here",
	}
	for _, f := range []scrapper.PlatformUserFact{
		scrapper.PlatformUserFactPlatformId,
		scrapper.PlatformUserFactDisplayName,
		scrapper.PlatformUserFactEmail,
		scrapper.PlatformUserFactComment,
		scrapper.PlatformUserFactDisabled,
		scrapper.PlatformUserFactCreatedAt,
		scrapper.PlatformUserFactLastLoginAt,
		scrapper.PlatformUserFactDefaultRole,
	} {
		result.Skip(f, "a Fabric database user does not state it")
	}
	if db.RolesErr != nil {
		result.Skip(scrapper.PlatformUserFactRoles, rolesSkipReason(db.RolesErr))
	}

	for _, d := range db.Users {
		if strings.HasSuffix(d.Type.String, "_GROUP") {
			// A group is not a login; its members sign in as themselves.
			continue
		}
		u := &scrapper.PlatformUser{Login: d.Name, Type: d.Type.String}
		guid, hasGUID := sidGUID(d.Sid)
		switch {
		case hasGUID && loginsByGUID[guid] != "":
			u.Login = loginsByGUID[guid]
		case d.OwnLogin.Valid && d.OwnLogin.String != "":
			u.Login = d.OwnLogin.String
		}
		if hasGUID {
			u.Roles = db.RolesBySid[guid]
		}
		result.Users = append(result.Users, u)
	}
	return result
}

// rolesSkipReason says why database role memberships could not be read.
func rolesSkipReason(err error) string {
	switch {
	case dwhexecfabric.IsPermissionError(err):
		return "the identity may not read database role memberships (VIEW DEFINITION on the database): " + err.Error()
	case dwhexecfabric.IsUnavailableError(err):
		return "this Fabric SQL surface has no sys.database_role_members: " + err.Error()
	default:
		return "reading database role memberships failed: " + err.Error()
	}
}

// sidGUID reads the GUID an Entra identity's SID encodes, from the 0x-prefixed
// hex CONVERT(varchar, sid, 1) gives: the 16 bytes of the GUID in .NET's
// layout, whose first three groups are little-endian.
func sidGUID(sid string) (string, bool) {
	b, err := hex.DecodeString(strings.TrimPrefix(strings.ToLower(sid), "0x"))
	if err != nil || len(b) != 16 {
		return "", false
	}
	return fmt.Sprintf("%02x%02x%02x%02x-%02x%02x-%02x%02x-%x-%x",
		b[3], b[2], b[1], b[0], b[5], b[4], b[7], b[6], b[8:10], b[10:16]), true
}

func selectRows[T any](ctx context.Context, executor *dwhexecfabric.FabricExecutor, query string) ([]*T, error) {
	rows, err := executor.QueryRows(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scrapper.ScanAll[T](ctx, rows, "fabric platform users")
}
