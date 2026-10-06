package fabric

import (
	"context"
	"database/sql"
	"net/http"
	"strings"
	"testing"

	dwhexecfabric "github.com/getsynq/dwhsupport/exec/fabric"
	"github.com/getsynq/dwhsupport/scrapper"
	mssql "github.com/microsoft/go-mssqldb"
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
	result, err := buildPlatformUsers(context.Background(), roleAssignments(), nil, dbUsers(), nil, testTenant, "wh")
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
	result, err := buildPlatformUsers(context.Background(), roleAssignments()[:2], nil, dbUsers(), nil, testTenant, "wh")
	require.NoError(t, err)
	assert.Equal(t, scrapper.PlatformUsersComplete, result.Source(sourceWorkspaceRoleAssignments).Completeness)
	assert.Equal(t, scrapper.PlatformUsersComplete, result.Reconcile().Completeness)
}

func TestBuildPlatformUsersDatabaseRefused(t *testing.T) {
	result, err := buildPlatformUsers(
		context.Background(),
		roleAssignments()[:2],
		nil,
		nil,
		errors.New("The SELECT permission was denied"),
		testTenant,
		"wh",
	)
	require.NoError(t, err)
	db := result.Source(sourceDatabasePrincipals)
	require.NotNil(t, db)
	assert.Contains(t, db.Refused, "permission was denied")
	assert.Len(t, result.Source(sourceWorkspaceRoleAssignments).Users, 2)

	db2 := &databaseUsers{Users: dbUsers().Users, RolesErr: errors.New("denied")}
	result, err = buildPlatformUsers(context.Background(), roleAssignments()[:2], nil, db2, nil, testTenant, "wh")
	require.NoError(t, err)
	assert.True(t, result.Source(sourceDatabasePrincipals).IsSkipped(scrapper.PlatformUserFactRoles))
}

func TestBuildPlatformUsersAPIRefused(t *testing.T) {
	refused := errors.WithStack(&dwhexecfabric.APIError{StatusCode: http.StatusForbidden, ErrorCode: "InsufficientWorkspaceRole"})
	result, err := buildPlatformUsers(context.Background(), nil, refused, dbUsers(), nil, testTenant, "wh")
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

	tokenOnly, err := buildPlatformUsers(context.Background(), nil, dwhexecfabric.ErrNoAPICredential, dbUsers(), nil, testTenant, "wh")
	require.NoError(t, err)
	assert.Contains(t, tokenOnly.Sources[0].Unavailable, "pre-acquired SQL access token", "no grant lets a SQL token call the API")
}

func TestBuildPlatformUsersBothRefused(t *testing.T) {
	refused := errors.WithStack(&dwhexecfabric.APIError{StatusCode: http.StatusForbidden, ErrorCode: "InsufficientWorkspaceRole"})
	result, err := buildPlatformUsers(context.Background(), nil, refused, nil, permissionDenied(), testTenant, "wh")
	require.NoError(t, err, "every source refused is a result, not an error")
	assert.False(t, result.Answered())
	assert.NotEmpty(t, result.Source(sourceWorkspaceRoleAssignments).Refused)
	assert.NotEmpty(t, result.Source(sourceDatabasePrincipals).Refused)
	reconciled := result.Reconcile()
	assert.Contains(t, reconciled.Refused, "InsufficientWorkspaceRole")
	assert.Contains(t, reconciled.Refused, "permission was denied")
}

func TestBuildPlatformUsersNothingAnsweredAndSomethingFailedIsAnError(t *testing.T) {
	_, err := buildPlatformUsers(context.Background(), nil, dwhexecfabric.ErrNoAPICredential, nil,
		errors.New("login error: Login failed for token-identified principal"), testTenant, "wh")
	require.Error(t, err, "a connection that fails outright is the call's failure")

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = buildPlatformUsers(ctx, roleAssignments(), nil, dbUsers(), nil, testTenant, "wh")
	assert.ErrorIs(t, err, context.Canceled)
}

func permissionDenied() error {
	return errors.WithStack(mssql.Error{Number: 229, Message: "The SELECT permission was denied on the object 'database_principals'"})
}

func TestBuildPlatformUsersClassifiesTheAPI(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		state func(*scrapper.PlatformUserListing) string
	}{
		{"missing workspace role", errors.WithStack(&dwhexecfabric.APIError{StatusCode: http.StatusForbidden}),
			func(l *scrapper.PlatformUserListing) string { return l.Refused }},
		{"identity not accepted", errors.WithStack(&dwhexecfabric.APIError{StatusCode: http.StatusUnauthorized}),
			func(l *scrapper.PlatformUserListing) string { return l.Refused }},
		{"sql token only", errors.WithStack(&apiUnavailableError{err: dwhexecfabric.ErrNoAPICredential}),
			func(l *scrapper.PlatformUserListing) string { return l.Unavailable }},
		{"bare sql token error", dwhexecfabric.ErrNoAPICredential,
			func(l *scrapper.PlatformUserListing) string { return l.Unavailable }},
		{"host names no workspace", &apiUnavailableError{err: errors.New("not a Fabric endpoint host")},
			func(l *scrapper.PlatformUserListing) string { return l.Unavailable }},
		{"server error", errors.WithStack(&dwhexecfabric.APIError{StatusCode: http.StatusBadGateway}),
			func(l *scrapper.PlatformUserListing) string { return l.Failed }},
		{"timeout", context.DeadlineExceeded,
			func(l *scrapper.PlatformUserListing) string { return l.Failed }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := buildPlatformUsers(context.Background(), nil, tc.err, dbUsers(), nil, testTenant, "wh")
			require.NoError(t, err)
			api := result.Source(sourceWorkspaceRoleAssignments)
			assert.NotEmpty(t, tc.state(api), "refused=%q unavailable=%q failed=%q", api.Refused, api.Unavailable, api.Failed)
			assert.True(t, result.Source(sourceDatabasePrincipals).Answered())
		})
	}
}

func TestBuildPlatformUsersClassifiesTheDatabase(t *testing.T) {
	for _, tc := range []struct {
		name  string
		err   error
		state func(*scrapper.PlatformUserListing) string
	}{
		{"permission", permissionDenied(), func(l *scrapper.PlatformUserListing) string { return l.Refused }},
		{"missing view", errors.WithStack(mssql.Error{Number: 208, Message: "Invalid object name 'sys.database_principals'."}),
			func(l *scrapper.PlatformUserListing) string { return l.Unavailable }},
		{"not supported on fabric", errors.WithStack(mssql.Error{Number: 15868, Message: "FUNCTION 'SUSER_SID' is not supported."}),
			func(l *scrapper.PlatformUserListing) string { return l.Unavailable }},
		{"dropped connection", errors.New("read tcp: connection reset by peer"),
			func(l *scrapper.PlatformUserListing) string { return l.Failed }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result, err := buildPlatformUsers(context.Background(), roleAssignments()[:2], nil, nil, tc.err, testTenant, "wh")
			require.NoError(t, err)
			db := result.Source(sourceDatabasePrincipals)
			assert.NotEmpty(t, tc.state(db), "refused=%q unavailable=%q failed=%q", db.Refused, db.Unavailable, db.Failed)
		})
	}
}

func TestBuildPlatformUsersRolesFailingIsSkipped(t *testing.T) {
	for _, tc := range []struct {
		name, reason string
		err          error
	}{
		{"permission", "VIEW DEFINITION", permissionDenied()},
		{"missing view", "has no sys.database_role_members", errors.WithStack(mssql.Error{Number: 208, Message: "Invalid object name"})},
		{"other", "failed", errors.New("connection reset")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := dbUsers()
			db.RolesErr = tc.err
			result, err := buildPlatformUsers(context.Background(), roleAssignments()[:2], nil, db, nil, testTenant, "wh")
			require.NoError(t, err)
			src := result.Source(sourceDatabasePrincipals)
			require.True(t, src.Answered(), "a fact never fails the listing")
			require.True(t, src.IsSkipped(scrapper.PlatformUserFactRoles))
			for _, f := range src.SkippedFacts {
				if f.Fact == scrapper.PlatformUserFactRoles {
					assert.Contains(t, f.Reason, tc.reason)
				}
			}
		})
	}
}

// A source that failed for a reason no grant fixes must not read as refused:
// a caller would tell the customer to grant a workspace role that would not
// help.
func TestBuildPlatformUsersFailureIsNotRefusal(t *testing.T) {
	unavailableAPI := errors.WithStack(&dwhexecfabric.APIError{StatusCode: http.StatusServiceUnavailable, ErrorCode: "ServiceUnavailable"})
	result, err := buildPlatformUsers(context.Background(), nil, unavailableAPI, dbUsers(), nil, testTenant, "wh")
	require.NoError(t, err)
	api := result.Source(sourceWorkspaceRoleAssignments)
	assert.Empty(t, api.Refused, "a 503 is not a missing grant")
	assert.Contains(t, api.Failed, "ServiceUnavailable")

	result, err = buildPlatformUsers(context.Background(), nil, context.DeadlineExceeded, dbUsers(), nil, testTenant, "wh")
	require.NoError(t, err)
	api = result.Source(sourceWorkspaceRoleAssignments)
	assert.Empty(t, api.Refused, "a timeout is not a missing grant")
	assert.NotEmpty(t, api.Failed)

	result, err = buildPlatformUsers(
		context.Background(),
		roleAssignments()[:2],
		nil,
		nil,
		errors.New("read tcp: connection reset by peer"),
		testTenant,
		"wh",
	)
	require.NoError(t, err)
	db := result.Source(sourceDatabasePrincipals)
	assert.Empty(t, db.Refused, "a dropped connection is not a missing grant")
	assert.Contains(t, db.Failed, "connection reset")
}
