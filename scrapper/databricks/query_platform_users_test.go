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

func TestQueryPlatformUsersRefusedUsersIsAPermissionError(t *testing.T) {
	fake := platformUsersWorkspace()
	fake.denyUsers = true

	s := fake.start(t)
	users, err := scrappertest.OnlyPlatformUserSource(s.QueryPlatformUsers(context.Background()))
	require.Error(t, err)
	assert.Nil(t, users)
	assert.True(t, s.IsPermissionError(err), "a refused listing is a permission error, not an empty listing: %v", err)
}

func TestQueryPlatformUsersRateLimitFailsTheListing(t *testing.T) {
	for _, endpoint := range []string{"/Users", "/ServicePrincipals"} {
		t.Run(endpoint, func(t *testing.T) {
			fake := platformUsersWorkspace()
			fake.pacing = &fastPacing
			fake.refuse = func(r *http.Request) *refusedRequest {
				if strings.HasSuffix(r.URL.Path, "/scim/v2"+endpoint) {
					return rateLimited()
				}
				return nil
			}

			s := fake.start(t)
			users, err := scrappertest.OnlyPlatformUserSource(s.QueryPlatformUsers(context.Background()))
			require.Error(t, err)
			assert.Nil(t, users, "a listing cut off by the quota would read as principals that were removed")
			assert.True(t, dwhexecdatabricks.IsRateLimitError(err), "%v", err)
			assert.False(t, s.IsPermissionError(err), "a rate limit is not a missing grant: %v", err)
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
}
