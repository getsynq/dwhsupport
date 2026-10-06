package fabric

import (
	"database/sql"
	"net/http"
	"strings"
	"testing"

	dwhexecfabric "github.com/getsynq/dwhsupport/exec/fabric"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testTenant  = "00000000-aaaa-4bbb-8ccc-000000000001"
	testAppID   = "1b2c3d4e-5f60-4718-9293-a4b5c6d7e8f9"
	testAppSid  = "0x4E3D2C1B605F18479293A4B5C6D7E8F9"
	testUserOID = "11111111-2222-3333-4444-555555555555"
	testUserSid = "0x11111111222233334444555555555555"
)

func TestSidGUID(t *testing.T) {
	guid, ok := sidGUID(testAppSid)
	require.True(t, ok)
	assert.Equal(t, testAppID, guid, "the first three groups are little-endian")

	_, ok = sidGUID("0x01")
	assert.False(t, ok, "dbo's SID is not a GUID")
	_, ok = sidGUID("not hex")
	assert.False(t, ok)
	_, ok = sidGUID("")
	assert.False(t, ok)
}

func roleAssignments() []*dwhexecfabric.WorkspaceRoleAssignment {
	user := dwhexecfabric.WorkspacePrincipal{ID: testUserOID, DisplayName: "Jane Doe", Type: "User"}
	user.UserDetails = &struct {
		UserPrincipalName string `json:"userPrincipalName"`
	}{UserPrincipalName: "jane@example.com"}
	sp := dwhexecfabric.WorkspacePrincipal{ID: "99999999-0000-0000-0000-000000000000", DisplayName: "loader", Type: "ServicePrincipal"}
	sp.ServicePrincipalDetails = &struct {
		AadAppID string `json:"aadAppId"`
	}{AadAppID: strings.ToUpper(testAppID)}
	group := dwhexecfabric.WorkspacePrincipal{ID: "g1", DisplayName: "Analysts", Type: "Group"}
	noUpn := dwhexecfabric.WorkspacePrincipal{ID: "u2", DisplayName: "No UPN", Type: "User"}
	return []*dwhexecfabric.WorkspaceRoleAssignment{
		{Principal: user, Role: "Admin"},
		{Principal: sp, Role: "Contributor"},
		{Principal: group, Role: "Viewer"},
		{Principal: noUpn, Role: "Viewer"},
	}
}

func dbUsers() *databaseUsers {
	return &databaseUsers{
		Users: []*databaseUserRow{
			{Name: "my-fabric-scraper", Sid: testAppSid, Type: sql.NullString{Valid: true, String: "EXTERNAL_USER"},
				OwnLogin: sql.NullString{Valid: true, String: testAppID + "@" + testTenant}},
			{Name: "Jane Doe", Sid: testUserSid, Type: sql.NullString{Valid: true, String: "EXTERNAL_USER"}},
			{Name: "Analysts", Sid: "0x01", Type: sql.NullString{Valid: true, String: "EXTERNAL_GROUP"}},
		},
		RolesBySid: map[string][]string{
			testAppID:   {"wh.db_datareader"},
			testUserOID: {"wh.db_owner"},
		},
	}
}

func TestBuildPlatformUsersReadsBothSources(t *testing.T) {
	result, err := buildPlatformUsers(roleAssignments(), nil, dbUsers(), nil, testTenant, "wh")
	require.NoError(t, err)
	require.Len(t, result.Sources, 2)

	api := result.Sources[0]
	assert.Equal(t, sourceWorkspaceRoleAssignments, api.Source, "the API is the more trusted source for identity")
	assert.Equal(t, scrapper.PlatformUserSourceAPI, api.Kind)
	assert.Equal(t, scrapper.PlatformUsersLimited, api.Completeness, "a group's members are not listed")
	assert.Contains(t, api.CompletenessReason, `Group "Analysts"`)
	assert.Contains(t, api.CompletenessReason, `User "No UPN"`)
	require.Len(t, api.Users, 2)
	sp, jane := api.Users[0], api.Users[1]
	assert.Equal(t, testAppID+"@"+testTenant, sp.Login, "query history names a service principal <app id>@<tenant id>")
	assert.Equal(t, "ServicePrincipal", sp.Type)
	assert.Equal(t, "loader", sp.DisplayName)
	assert.Equal(t, []string{"Contributor"}, sp.Roles, "database roles stay on the database source")
	assert.Equal(t, "jane@example.com", jane.Login)
	assert.Equal(t, testUserOID, jane.PlatformId)
	assert.Equal(t, []string{"Admin"}, jane.Roles)
	assert.True(t, api.IsSkipped(scrapper.PlatformUserFactEmail))

	db := result.Sources[1]
	assert.Equal(t, sourceDatabasePrincipals, db.Source)
	assert.Equal(t, scrapper.PlatformUserSourceSQL, db.Kind)
	assert.Equal(t, scrapper.PlatformUsersLimited, db.Completeness)
	require.Len(t, db.Users, 2, "a group is not a login")
	assert.Equal(t, testAppID+"@"+testTenant, db.Users[0].Login)
	assert.Equal(t, []string{"wh.db_datareader"}, db.Users[0].Roles)
	assert.Equal(t, "jane@example.com", db.Users[1].Login, "a database user carries the login the API gives the same identity")
	assert.Equal(t, []string{"wh.db_owner"}, db.Users[1].Roles)
	assert.Empty(t, db.Users[1].DisplayName, "no facts are copied between sources")

	reconciled := result.Reconcile()
	require.Len(t, reconciled.Users, 2, "the two sources name the same identities")
	assert.Equal(t, []string{"Admin", "wh.db_owner"}, reconciled.Users[1].Roles)
}

func TestBuildPlatformUsersOnlyUsersIsComplete(t *testing.T) {
	result, err := buildPlatformUsers(roleAssignments()[:2], nil, dbUsers(), nil, testTenant, "wh")
	require.NoError(t, err)
	assert.Equal(t, scrapper.PlatformUsersComplete, result.Source(sourceWorkspaceRoleAssignments).Completeness)
	assert.Equal(t, scrapper.PlatformUsersComplete, result.Reconcile().Completeness)
}

func TestBuildPlatformUsersDatabaseRefused(t *testing.T) {
	result, err := buildPlatformUsers(roleAssignments()[:2], nil, nil, errors.New("The SELECT permission was denied"), testTenant, "wh")
	require.NoError(t, err)
	db := result.Source(sourceDatabasePrincipals)
	require.NotNil(t, db)
	assert.Contains(t, db.Refused, "permission was denied")
	assert.Len(t, result.Source(sourceWorkspaceRoleAssignments).Users, 2)

	db2 := &databaseUsers{Users: dbUsers().Users, RolesErr: errors.New("denied")}
	result, err = buildPlatformUsers(roleAssignments()[:2], nil, db2, nil, testTenant, "wh")
	require.NoError(t, err)
	assert.True(t, result.Source(sourceDatabasePrincipals).IsSkipped(scrapper.PlatformUserFactRoles))
}

func TestBuildPlatformUsersAPIRefused(t *testing.T) {
	refused := errors.WithStack(&dwhexecfabric.APIError{StatusCode: http.StatusForbidden, ErrorCode: "InsufficientWorkspaceRole"})
	result, err := buildPlatformUsers(nil, refused, dbUsers(), nil, testTenant, "wh")
	require.NoError(t, err, "a refused API leaves the database source, not an error")
	require.Len(t, result.Sources, 2)
	api := result.Sources[0]
	assert.Equal(t, sourceWorkspaceRoleAssignments, api.Source)
	assert.Contains(t, api.Refused, "InsufficientWorkspaceRole")
	assert.Empty(t, api.Users)

	db := result.Sources[1]
	require.Len(t, db.Users, 2)
	assert.Equal(t, testAppID+"@"+testTenant, db.Users[0].Login, "the connection's own login comes from SUSER_SNAME()")
	assert.Equal(t, []string{"wh.db_datareader"}, db.Users[0].Roles)
	assert.Equal(t, "Jane Doe", db.Users[1].Login, "without the API a database user keeps its database name")

	reconciled := result.Reconcile()
	assert.Equal(t, scrapper.PlatformUsersLimited, reconciled.Completeness)
	assert.Contains(t, reconciled.CompletenessReason, sourceWorkspaceRoleAssignments+" refused")

	tokenOnly, err := buildPlatformUsers(nil, dwhexecfabric.ErrNoAPICredential, dbUsers(), nil, testTenant, "wh")
	require.NoError(t, err)
	assert.Contains(t, tokenOnly.Sources[0].Refused, "pre-acquired SQL access token")
}

func TestBuildPlatformUsersBothRefused(t *testing.T) {
	refused := errors.WithStack(&dwhexecfabric.APIError{StatusCode: http.StatusForbidden})
	_, err := buildPlatformUsers(nil, refused, nil, errors.New("The SELECT permission was denied"), testTenant, "wh")
	require.Error(t, err)
	var apiErr *dwhexecfabric.APIError
	assert.True(t, errors.As(err, &apiErr), "the API's refusal names the grant that lists every user")

	_, err = buildPlatformUsers(nil, errors.New("timeout"), nil, errors.New("The SELECT permission was denied"), testTenant, "wh")
	require.Error(t, err)
	assert.True(t, dwhexecfabric.IsPermissionError(err), "the database's refusal when the API only failed")
}
