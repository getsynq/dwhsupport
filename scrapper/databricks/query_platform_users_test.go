package databricks

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	serviceiam "github.com/databricks/databricks-sdk-go/service/iam"
	dwhexecdatabricks "github.com/getsynq/dwhsupport/exec/databricks"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const loaderApplicationId = "8b2d6a3e-1f4c-4b7a-9c0e-2d5f6a7b8c9d"

func platformUsersWorkspace() *fakeWorkspace {
	fake := newFakeWorkspace()
	fake.users = []serviceiam.User{
		{
			Id: "101", UserName: "analyst@example.com", DisplayName: "Ana Lyst", Active: true,
			Emails: []serviceiam.ComplexValue{
				{Value: "secondary@example.com"},
				{Value: "analyst@example.com", Primary: true},
			},
			Groups: []serviceiam.ComplexValue{
				{Display: "reporters", Type: "direct"},
				{Display: "all-staff", Type: "indirect"},
				{Display: "admins"},
			},
		},
		{
			Id: "102", UserName: "leaver@example.com", Active: false, ForceSendFields: []string{"Active"},
			Emails: []serviceiam.ComplexValue{{Value: "leaver@example.com"}},
		},
	}
	fake.servicePrincipals = []serviceiam.ServicePrincipal{
		{
			Id: "201", ApplicationId: loaderApplicationId, DisplayName: "loader", Active: true,
			Groups: []serviceiam.ComplexValue{{Display: "LOADER", Type: "direct"}},
		},
	}
	return fake
}

func TestQueryPlatformUsersListsUsersAndServicePrincipals(t *testing.T) {
	fake := platformUsersWorkspace()
	users, err := scrappertest.OnlyPlatformUserSource(fake.start(t).QueryPlatformUsers(context.Background()))
	require.NoError(t, err)

	assert.Equal(t, scrapper.PlatformUsersComplete, users.Completeness)
	require.Len(t, users.Users, 3)

	no, yes := false, true
	assert.Equal(t, &scrapper.PlatformUser{
		Login:      loaderApplicationId,
		PlatformId: "201", Type: scrapper.PlatformUserTypeDatabricksServicePrincipal,
		DisplayName: "loader", Disabled: &no, Roles: []string{"LOADER"},
	}, users.Users[0], "a service principal's login is its application id, which query history reports as the user")
	assert.Equal(t, &scrapper.PlatformUser{
		Login:      "analyst@example.com",
		PlatformId: "101", Type: scrapper.PlatformUserTypeDatabricksUser,
		Email: "analyst@example.com", DisplayName: "Ana Lyst", Disabled: &no,
		Roles: []string{"admins", "reporters"},
	}, users.Users[1], "the primary email wins, and a group reached through a nested group is not the user's own")
	assert.Equal(t, &scrapper.PlatformUser{
		Login:      "leaver@example.com",
		PlatformId: "102", Type: scrapper.PlatformUserTypeDatabricksUser,
		Email: "leaver@example.com", Disabled: &yes,
	}, users.Users[2])

	for _, fact := range []scrapper.PlatformUserFact{
		scrapper.PlatformUserFactCreatedAt, scrapper.PlatformUserFactLastLoginAt,
		scrapper.PlatformUserFactDefaultRole, scrapper.PlatformUserFactComment,
	} {
		assert.Truef(t, users.IsSkipped(fact), "%s is not on SCIM and must say so", fact)
	}
}

func TestQueryPlatformUsersMissingActiveIsUnknown(t *testing.T) {
	fake := platformUsersWorkspace()
	fake.users = []serviceiam.User{{Id: "1", UserName: "nobody@example.com"}}
	fake.servicePrincipals = nil

	users, err := scrappertest.OnlyPlatformUserSource(fake.start(t).QueryPlatformUsers(context.Background()))
	require.NoError(t, err)
	require.Len(t, users.Users, 1)
	assert.Nil(t, users.Users[0].Disabled, "a response that left out active says nothing about whether the user is disabled")
}

func TestQueryPlatformUsersPagesThroughEveryPrincipal(t *testing.T) {
	fake := newFakeWorkspace()
	for i := range 1203 {
		fake.users = append(fake.users, serviceiam.User{Id: fmt.Sprint(i), UserName: fmt.Sprintf("user%04d@example.com", i), Active: true})
	}
	for i := range 7 {
		fake.servicePrincipals = append(fake.servicePrincipals, serviceiam.ServicePrincipal{
			Id: fmt.Sprint(5000 + i), ApplicationId: fmt.Sprintf("00000000-0000-0000-0000-%012d", i), Active: true,
		})
	}
	// The server answers fewer than asked, so the listing has to follow startIndex rather
	// than stop at the first short page.
	fake.pageSize = 300

	users, err := scrappertest.OnlyPlatformUserSource(fake.start(t).QueryPlatformUsers(context.Background()))
	require.NoError(t, err)
	assert.Len(t, users.Users, 1210)
	for _, size := range fake.scimPageSizes {
		assert.Equal(t, fmt.Sprint(scimPageSize), size, "every SCIM page asks for the same bounded page size")
	}
}

func TestQueryPlatformUsersRefusedServicePrincipalsLimitTheListing(t *testing.T) {
	fake := platformUsersWorkspace()
	fake.denyServicePrincipals = true

	users, err := scrappertest.OnlyPlatformUserSource(fake.start(t).QueryPlatformUsers(context.Background()))
	require.NoError(t, err)
	assert.Equal(t, scrapper.PlatformUsersLimited, users.Completeness)
	assert.Contains(t, users.CompletenessReason, "service principal")
	assert.Contains(t, users.CompletenessReason, "admins group")
	require.Len(t, users.Users, 2)
	for _, u := range users.Users {
		assert.Equal(t, scrapper.PlatformUserTypeDatabricksUser, u.Type)
	}
}

func TestQueryPlatformUsersRefusedUsersIsARefusedSource(t *testing.T) {
	fake := platformUsersWorkspace()
	fake.denyUsers = true

	result, err := fake.start(t).QueryPlatformUsers(context.Background())
	require.NoError(t, err, "a missing grant is a state of the result, never a failed fetch")
	users, err := scrappertest.OnlyPlatformUserSource(result, nil)
	require.NoError(t, err)
	assert.Equal(t, platformUsersSource, users.Source)
	assert.Contains(t, users.Refused, "Only workspace admins can list users")
	assert.Empty(t, users.Unavailable)
	assert.Empty(t, users.Failed)
	assert.Empty(t, users.Users)
	assert.Empty(t, users.Completeness, "a source that did not answer claims no completeness")
}

// refuseScim answers one SCIM endpoint with the given refusal and serves the rest.
func refuseScim(endpoint string, refused *refusedRequest) func(r *http.Request) *refusedRequest {
	return func(r *http.Request) *refusedRequest {
		if strings.HasSuffix(r.URL.Path, "/scim/v2"+endpoint) {
			return refused
		}
		return nil
	}
}

func TestQueryPlatformUsersAbsentUserListingIsAnUnavailableSource(t *testing.T) {
	for name, refused := range map[string]*refusedRequest{
		"404":              {status: http.StatusNotFound, errorCode: "ENDPOINT_NOT_FOUND", message: "No API found for 'GET /preview/scim/v2/Users'"},
		"FEATURE_DISABLED": {status: http.StatusBadRequest, errorCode: "FEATURE_DISABLED", message: "SCIM is not enabled for this workspace"},
	} {
		t.Run(name, func(t *testing.T) {
			fake := platformUsersWorkspace()
			fake.refuse = refuseScim("/Users", refused)

			users, err := scrappertest.OnlyPlatformUserSource(fake.start(t).QueryPlatformUsers(context.Background()))
			require.NoError(t, err)
			assert.Contains(t, users.Unavailable, refused.message)
			assert.Empty(t, users.Refused)
			assert.Empty(t, users.Failed)
		})
	}
}

func TestQueryPlatformUsersRateLimitedUsersFailsTheCall(t *testing.T) {
	fake := platformUsersWorkspace()
	fake.pacing = &fastPacing
	fake.refuse = refuseScim("/Users", rateLimited())

	s := fake.start(t)
	result, err := s.QueryPlatformUsers(context.Background())
	require.Error(t, err, "nothing answered and the one source failed: that is a failure of the call")
	assert.Nil(t, result)
	assert.True(t, dwhexecdatabricks.IsRateLimitError(err), "%v", err)
	assert.False(t, s.IsPermissionError(err), "a rate limit is not a missing grant: %v", err)
}

func TestQueryPlatformUsersServicePrincipalsNotAnsweringLimitTheListing(t *testing.T) {
	for name, refused := range map[string]*refusedRequest{
		// A rate limit the pacing gave up on: the users already read are kept, and the
		// listing says it is missing the service principals rather than passing as complete.
		"rate limited":     rateLimited(),
		"FEATURE_DISABLED": {status: http.StatusBadRequest, errorCode: "FEATURE_DISABLED", message: "Service principals are not enabled"},
	} {
		t.Run(name, func(t *testing.T) {
			fake := platformUsersWorkspace()
			fake.pacing = &fastPacing
			fake.refuse = refuseScim("/ServicePrincipals", refused)

			users, err := scrappertest.OnlyPlatformUserSource(fake.start(t).QueryPlatformUsers(context.Background()))
			require.NoError(t, err)
			assert.Equal(t, scrapper.PlatformUsersLimited, users.Completeness)
			assert.Contains(t, users.CompletenessReason, "service principal")
			require.Len(t, users.Users, 2)
			for _, u := range users.Users {
				assert.Equal(t, scrapper.PlatformUserTypeDatabricksUser, u.Type)
			}
		})
	}
}

func TestQueryPlatformUsersEmptyWorkspaceIsEmptyNotComplete(t *testing.T) {
	users, err := scrappertest.OnlyPlatformUserSource(newFakeWorkspace().start(t).QueryPlatformUsers(context.Background()))
	require.NoError(t, err)
	assert.Equal(t, scrapper.PlatformUsersEmpty, users.Completeness)
}

func TestCapabilitiesAdvertisePlatformUsers(t *testing.T) {
	capability := newFakeWorkspace().start(t).Capabilities().PlatformUsers
	assert.True(t, capability.Supported)
	assert.NotEmpty(t, capability.Grant)
	// A token issued for a set of scopes is refused with "does not have required scopes:
	// scim" whatever its principal may do, so the grant names the scope as well.
	assert.Contains(t, capability.Grant, "admins group")
	assert.Contains(t, capability.Grant, "scopes include scim")
}
