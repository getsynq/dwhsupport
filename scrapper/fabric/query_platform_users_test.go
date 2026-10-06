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
	testTenant  = "8863585d-cb0b-4ecf-9ce3-318e3295156a"
	testAppID   = "51da975c-167d-4049-911e-7192342601e5"
	testAppSid  = "0x5C97DA517D164940911E7192342601E5"
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

func TestBuildPlatformUsersFromAssignments(t *testing.T) {
	db := &databaseUsers{RolesBySid: map[string][]string{
		testAppID:                              {"wh.db_datareader"},
		testUserOID:                            {"wh.db_owner"},
		"00000000-0000-0000-0000-000000000000": {"wh.db_ddladmin"},
	}}
	users, err := buildPlatformUsers(roleAssignments(), nil, db, nil, testTenant, "wh")
	require.NoError(t, err)

	assert.Equal(t, scrapper.PlatformUsersLimited, users.Completeness, "a group's members are not listed")
	assert.Contains(t, users.CompletenessReason, `Group "Analysts"`)
	assert.Contains(t, users.CompletenessReason, `User "No UPN"`)
	require.Len(t, users.Users, 2)

	sp, jane := users.Users[0], users.Users[1]
	assert.Equal(t, testAppID+"@"+testTenant, sp.Login, "query history names a service principal <app id>@<tenant id>")
	assert.Equal(t, "ServicePrincipal", sp.Type)
	assert.Equal(t, "loader", sp.DisplayName)
	assert.Equal(t, []string{"Contributor", "wh.db_datareader"}, sp.Roles)

	assert.Equal(t, "jane@example.com", jane.Login)
	assert.Equal(t, testUserOID, jane.PlatformId)
	assert.Equal(t, []string{"Admin", "wh.db_owner"}, jane.Roles)

	assert.True(t, users.IsSkipped(scrapper.PlatformUserFactEmail))
	assert.False(t, users.IsSkipped(scrapper.PlatformUserFactRoles))
}

func TestBuildPlatformUsersOnlyUsersIsComplete(t *testing.T) {
	users, err := buildPlatformUsers(roleAssignments()[:2], nil, &databaseUsers{RolesBySid: map[string][]string{}}, nil, testTenant, "wh")
	require.NoError(t, err)
	assert.Equal(t, scrapper.PlatformUsersComplete, users.Completeness)
}

func TestBuildPlatformUsersDatabaseRolesUnreadable(t *testing.T) {
	users, err := buildPlatformUsers(roleAssignments()[:2], nil, nil, errors.New("connection reset"), testTenant, "wh")
	require.NoError(t, err, "the database is only a source of extra roles when the API answered")
	assert.True(t, users.IsSkipped(scrapper.PlatformUserFactRoles))
	assert.Equal(t, []string{"Admin"}, users.Users[1].Roles, "workspace roles stay")

	users, err = buildPlatformUsers(roleAssignments()[:2], nil, &databaseUsers{RolesErr: errors.New("denied")}, nil, testTenant, "wh")
	require.NoError(t, err)
	assert.True(t, users.IsSkipped(scrapper.PlatformUserFactRoles))
}

func TestBuildPlatformUsersFallsBackToTheDatabase(t *testing.T) {
	refused := errors.WithStack(&dwhexecfabric.APIError{StatusCode: http.StatusForbidden, ErrorCode: "InsufficientWorkspaceRole"})
	db := &databaseUsers{
		Users: []*databaseUserRow{
			{Name: "coalesce-quality-fabric-scraper", Sid: testAppSid, Type: sql.NullString{Valid: true, String: "EXTERNAL_USER"},
				OwnLogin: sql.NullString{Valid: true, String: testAppID + "@" + testTenant}},
			{Name: "jane@example.com", Sid: testUserSid, Type: sql.NullString{Valid: true, String: "EXTERNAL_USER"}},
			{Name: "Analysts", Sid: "0x01", Type: sql.NullString{Valid: true, String: "EXTERNAL_GROUP"}},
		},
		RolesBySid: map[string][]string{testAppID: {"wh.db_datareader"}},
	}
	users, err := buildPlatformUsers(nil, refused, db, nil, testTenant, "wh")
	require.NoError(t, err, "a refused API leaves a limited listing, not an error")
	assert.Equal(t, scrapper.PlatformUsersLimited, users.Completeness)
	assert.Contains(t, users.CompletenessReason, "InsufficientWorkspaceRole")
	assert.Contains(t, users.CompletenessReason, "Member or Admin")
	require.Len(t, users.Users, 2, "a group is not a login")
	assert.Equal(t, testAppID+"@"+testTenant, users.Users[0].Login, "the connection's own login comes from SUSER_SNAME()")
	assert.Equal(t, []string{"wh.db_datareader"}, users.Users[0].Roles)
	assert.Equal(t, "jane@example.com", users.Users[1].Login)

	tokenOnly, err := buildPlatformUsers(nil, dwhexecfabric.ErrNoAPICredential, db, nil, testTenant, "wh")
	require.NoError(t, err)
	assert.Contains(t, tokenOnly.CompletenessReason, "pre-acquired SQL access token")
}

func TestBuildPlatformUsersBothRefused(t *testing.T) {
	refused := errors.WithStack(&dwhexecfabric.APIError{StatusCode: http.StatusForbidden})
	_, err := buildPlatformUsers(nil, refused, nil, errors.New("The SELECT permission was denied"), testTenant, "wh")
	require.Error(t, err)
	assert.True(t, dwhexecfabric.IsPermissionError(err), "the API's refusal names the grant that completes the listing")

	_, err = buildPlatformUsers(nil, errors.New("timeout"), nil, errors.New("The SELECT permission was denied"), testTenant, "wh")
	require.Error(t, err)
	assert.True(t, dwhexecfabric.IsPermissionError(err), "the database's refusal")
}
