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

// QueryPlatformUsers lists the workspace's users from its role assignments
// (Fabric REST API): Entra users by user principal name and service
// principals as <application id>@<tenant id>, which is how query history
// (queryinsights login_name) names them. Database roles of the connected
// database are added where the identity was added to it.
//
// Listing the role assignments needs the Member or Admin workspace role. When
// the API refuses, or the connection holds only a SQL access token, the
// listing falls back to the connected database's users, which hold only the
// identities added to it explicitly, and says it is limited.
func (e *FabricScrapper) QueryPlatformUsers(ctx context.Context) (*scrapper.PlatformUsers, error) {
	identity, err := dwhexecfabric.ParseHostIdentity(e.conf.Host)
	tenantID := e.conf.TenantID
	var assignments []*dwhexecfabric.WorkspaceRoleAssignment
	var apiErr error
	if err != nil {
		apiErr = err
	} else {
		if tenantID == "" {
			tenantID = identity.TenantID
		}
		assignments, apiErr = e.listRoleAssignments(ctx, identity.WorkspaceID)
	}

	db, dbErr := e.queryDatabaseUsers(ctx)
	return buildPlatformUsers(assignments, apiErr, db, dbErr, tenantID, e.databaseName())
}

func (e *FabricScrapper) listRoleAssignments(ctx context.Context, workspaceID string) ([]*dwhexecfabric.WorkspaceRoleAssignment, error) {
	client, err := dwhexecfabric.NewAPIClient(&e.conf.FabricConf)
	if err != nil {
		return nil, err
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

// buildPlatformUsers assembles the listing from the role assignments when the
// API answered, and from the database's users otherwise. Only when both are
// refused is the listing an error, the API's permission error first since its
// grant is the one that completes the listing.
func buildPlatformUsers(
	assignments []*dwhexecfabric.WorkspaceRoleAssignment,
	apiErr error,
	db *databaseUsers,
	dbErr error,
	tenantID string,
	databaseName string,
) (*scrapper.PlatformUsers, error) {
	if apiErr == nil {
		result := platformUsersFromAssignments(assignments, tenantID)
		addDatabaseRoles(result, assignments, db, dbErr)
		return result.Finish(), nil
	}
	if dbErr != nil {
		if dwhexecfabric.IsPermissionError(apiErr) {
			return nil, errors.Wrap(apiErr, "failed to list the workspace role assignments")
		}
		return nil, errors.Wrap(dbErr, "failed to list the database users")
	}
	return platformUsersFromDatabase(db, apiErr, databaseName).Finish(), nil
}

func platformUsersFromAssignments(assignments []*dwhexecfabric.WorkspaceRoleAssignment, tenantID string) *scrapper.PlatformUsers {
	result := &scrapper.PlatformUsers{Completeness: scrapper.PlatformUsersComplete}
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

// addDatabaseRoles adds the connected database's roles to the users that were
// added to it. A user's SID is the GUID of its Entra object id, a service
// principal's the GUID of its application id.
func addDatabaseRoles(result *scrapper.PlatformUsers, assignments []*dwhexecfabric.WorkspaceRoleAssignment, db *databaseUsers, dbErr error) {
	switch {
	case dbErr != nil:
		result.Skip(scrapper.PlatformUserFactRoles, "only workspace roles are listed: reading the database's users failed: "+dbErr.Error())
		return
	case db.RolesErr != nil:
		result.Skip(scrapper.PlatformUserFactRoles, "only workspace roles are listed: reading database role memberships failed: "+db.RolesErr.Error())
		return
	}
	byKey := map[string]*scrapper.PlatformUser{}
	for _, u := range result.Users {
		byKey[strings.ToLower(u.PlatformId)] = u
	}
	for _, a := range assignments {
		if d := a.Principal.ServicePrincipalDetails; d != nil && d.AadAppID != "" {
			if u, ok := byKey[strings.ToLower(a.Principal.ID)]; ok {
				byKey[strings.ToLower(d.AadAppID)] = u
			}
		}
	}
	for guid, roles := range db.RolesBySid {
		if u, ok := byKey[guid]; ok {
			u.Roles = append(u.Roles, roles...)
		}
	}
}

func platformUsersFromDatabase(db *databaseUsers, apiErr error, databaseName string) *scrapper.PlatformUsers {
	reason := "the workspace role assignments could not be read"
	if errors.Is(apiErr, dwhexecfabric.ErrNoAPICredential) {
		reason = "the connection uses a pre-acquired SQL access token, which cannot call the Fabric API to list the workspace role assignments"
	} else if apiErr != nil {
		reason += " (" + apiErr.Error() + ")"
	}
	result := &scrapper.PlatformUsers{
		Completeness: scrapper.PlatformUsersLimited,
		CompletenessReason: reason + "; listed instead are the users of database " + databaseName +
			", which holds only the identities added to it explicitly; grant " + platformUsersGrant,
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
		result.Skip(scrapper.PlatformUserFactRoles, "reading database role memberships failed: "+db.RolesErr.Error())
	}

	for _, d := range db.Users {
		if strings.HasSuffix(d.Type.String, "_GROUP") {
			// A group is not a login; its members sign in as themselves.
			continue
		}
		login := d.Name
		if d.OwnLogin.Valid && d.OwnLogin.String != "" {
			login = d.OwnLogin.String
		}
		u := &scrapper.PlatformUser{Login: login, Type: d.Type.String}
		if guid, ok := sidGUID(d.Sid); ok {
			u.Roles = db.RolesBySid[guid]
		}
		result.Users = append(result.Users, u)
	}
	return result
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
