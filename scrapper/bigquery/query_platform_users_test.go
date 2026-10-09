package bigquery

import (
	"context"
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scrappertest"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/cloudresourcemanager/v1"
	"google.golang.org/api/googleapi"
	iam "google.golang.org/api/iam/v1"
)

func TestParseIamMember(t *testing.T) {
	cases := []struct {
		member    string
		userType  string
		address   string
		isDeleted bool
		ok        bool
	}{
		{"user:jdoe@example.com", scrapper.PlatformUserTypeBigQueryUser, "jdoe@example.com", false, true},
		{"serviceAccount:loader@my-project.iam.gserviceaccount.com", scrapper.PlatformUserTypeBigQueryServiceAccount,
			"loader@my-project.iam.gserviceaccount.com", false, true},
		{"deleted:user:gone@example.com?uid=123456789", scrapper.PlatformUserTypeBigQueryUser, "gone@example.com", true, true},
		{"deleted:serviceAccount:old@my-project.iam.gserviceaccount.com?uid=42", scrapper.PlatformUserTypeBigQueryServiceAccount,
			"old@my-project.iam.gserviceaccount.com", true, true},
		{"group:analysts@example.com", "", "", false, false},
		{"deleted:group:analysts@example.com?uid=1", "", "", false, false},
		{"domain:example.com", "", "", false, false},
		{"allUsers", "", "", false, false},
		{"allAuthenticatedUsers", "", "", false, false},
		{"principal://iam.googleapis.com/locations/global/workforcePools/pool/subject/jdoe", "", "", false, false},
		{"principalSet://iam.googleapis.com/locations/global/workforcePools/pool/*", "", "", false, false},
		{"projectOwner:my-project", "", "", false, false},
		{"user:", "", "", false, false},
		{"", "", "", false, false},
	}
	for _, c := range cases {
		t.Run(c.member, func(t *testing.T) {
			userType, address, isDeleted, ok := parseIamMember(c.member)
			assert.Equal(t, c.ok, ok)
			assert.Equal(t, c.userType, userType)
			assert.Equal(t, c.address, address)
			assert.Equal(t, c.isDeleted, isDeleted)
		})
	}
}

func TestPlatformUsersFromPolicy(t *testing.T) {
	policy := &cloudresourcemanager.Policy{Bindings: []*cloudresourcemanager.Binding{
		{Role: "roles/bigquery.user", Members: []string{
			"user:jdoe@example.com",
			"group:analysts@example.com",
			"serviceAccount:loader@my-project.iam.gserviceaccount.com",
		}},
		{Role: "roles/bigquery.dataEditor", Members: []string{
			"serviceAccount:loader@my-project.iam.gserviceaccount.com",
			"deleted:serviceAccount:old@my-project.iam.gserviceaccount.com?uid=1",
		}},
		{Role: "roles/bigquery.dataViewer", Members: []string{
			// Recreated: a deleted binding of an address that is bound live belongs to the former account.
			"deleted:user:jdoe@example.com?uid=7",
			"allAuthenticatedUsers",
		}},
		{Role: "roles/bigquery.jobUser", Condition: &cloudresourcemanager.Expr{Expression: "request.time < timestamp('2030-01-01T00:00:00Z')"},
			Members: []string{"user:contractor@example.com"}},
		nil,
	}}

	users := platformUsersFromPolicy(policy, scrapper.PlatformUserSkipUnavailable).Finish()

	assert.Equal(t, platformUserSourceIamPolicy, users.Source)
	assert.Equal(t, scrapper.PlatformUserSourceAPI, users.Kind)
	assert.Equal(t, scrapper.PlatformUsersUnknown, users.Completeness)
	assert.Equal(t, iamPolicyCompletenessReason, users.CompletenessReason)
	for _, fact := range []scrapper.PlatformUserFact{
		scrapper.PlatformUserFactCreatedAt, scrapper.PlatformUserFactLastLoginAt, scrapper.PlatformUserFactDefaultRole,
		scrapper.PlatformUserFactPlatformId, scrapper.PlatformUserFactDisplayName, scrapper.PlatformUserFactComment,
		scrapper.PlatformUserFactDisabled,
	} {
		scrappertest.AssertSkipped(t, users, fact, scrapper.PlatformUserSkipUnavailable)
	}
	assert.False(t, users.IsSkipped(scrapper.PlatformUserFactRoles))

	require.Len(t, users.Users, 4)
	byLogin := map[string]*scrapper.PlatformUser{}
	for _, u := range users.Users {
		byLogin[u.Login] = u
	}

	jdoe := byLogin["jdoe@example.com"]
	require.NotNil(t, jdoe)
	assert.Equal(t, scrapper.PlatformUserTypeBigQueryUser, jdoe.Type)
	assert.Equal(t, "jdoe@example.com", jdoe.Email)
	assert.Equal(t, []string{"roles/bigquery.user"}, jdoe.Roles, "the former account's roles do not move to the live one")
	assert.Nil(t, jdoe.Disabled, "IAM does not say whether a person is disabled")

	loader := byLogin["loader@my-project.iam.gserviceaccount.com"]
	require.NotNil(t, loader)
	assert.Equal(t, scrapper.PlatformUserTypeBigQueryServiceAccount, loader.Type)
	assert.Equal(t, []string{"roles/bigquery.dataEditor", "roles/bigquery.user"}, loader.Roles)

	old := byLogin["old@my-project.iam.gserviceaccount.com"]
	require.NotNil(t, old)
	require.NotNil(t, old.Disabled)
	assert.True(t, *old.Disabled)
	assert.Equal(t, []string{"roles/bigquery.dataEditor"}, old.Roles)

	contractor := byLogin["contractor@example.com"]
	require.NotNil(t, contractor)
	assert.Equal(t, []string{"roles/bigquery.jobUser"}, contractor.Roles, "a conditional binding still binds the role")
}

func TestPlatformUsersFromPolicy_Empty(t *testing.T) {
	users := platformUsersFromPolicy(&cloudresourcemanager.Policy{}, scrapper.PlatformUserSkipUnavailable).Finish()
	assert.Equal(t, scrapper.PlatformUsersEmpty, users.Completeness)
	assert.Empty(t, users.Users)

	users = platformUsersFromPolicy(nil, scrapper.PlatformUserSkipUnavailable).Finish()
	assert.Equal(t, scrapper.PlatformUsersEmpty, users.Completeness)
}

func TestPlatformUsersFromServiceAccounts(t *testing.T) {
	users := platformUsersFromServiceAccounts([]*iam.ServiceAccount{
		{Email: "loader@my-project.iam.gserviceaccount.com", UniqueId: "1001", DisplayName: "Loader", Description: "Fivetran loads", Disabled: true},
		{Email: "unbound@my-project.iam.gserviceaccount.com", UniqueId: "1002"},
		{Email: ""},
		nil,
	}, scrapper.PlatformUserSkipUnavailable).Finish()

	assert.Equal(t, platformUserSourceServiceAccounts, users.Source)
	assert.Equal(t, scrapper.PlatformUserSourceAPI, users.Kind)
	assert.Equal(t, scrapper.PlatformUsersUnknown, users.Completeness)
	assert.Equal(t, serviceAccountsCompletenessReason, users.CompletenessReason)
	for _, fact := range []scrapper.PlatformUserFact{
		scrapper.PlatformUserFactCreatedAt, scrapper.PlatformUserFactLastLoginAt,
		scrapper.PlatformUserFactDefaultRole, scrapper.PlatformUserFactRoles,
	} {
		scrappertest.AssertSkipped(t, users, fact, scrapper.PlatformUserSkipUnavailable)
	}

	require.Len(t, users.Users, 2, "an unbound service account is a login too")
	loader := users.Users[0]
	assert.Equal(t, "loader@my-project.iam.gserviceaccount.com", loader.Login)
	assert.Equal(t, loader.Login, loader.Email)
	assert.Equal(t, scrapper.PlatformUserTypeBigQueryServiceAccount, loader.Type)
	assert.Equal(t, "1001", loader.PlatformId)
	assert.Equal(t, "Loader", loader.DisplayName)
	assert.Equal(t, "Fivetran loads", loader.Comment)
	require.NotNil(t, loader.Disabled)
	assert.True(t, *loader.Disabled)
	assert.Nil(t, loader.Roles)

	unbound := users.Users[1]
	require.NotNil(t, unbound.Disabled)
	assert.False(t, *unbound.Disabled)
}

func TestPlatformUsersFromSources(t *testing.T) {
	ctx := context.Background()
	accounts := []*iam.ServiceAccount{
		{Email: "loader@my-project.iam.gserviceaccount.com", UniqueId: "1001", DisplayName: "Loader"},
		{Email: "unbound@my-project.iam.gserviceaccount.com", UniqueId: "1002"},
		{Email: "recreated@my-project.iam.gserviceaccount.com", UniqueId: "1003"},
	}
	policy := &cloudresourcemanager.Policy{Bindings: []*cloudresourcemanager.Binding{
		{Role: "roles/bigquery.user", Members: []string{
			"user:jdoe@example.com",
			"serviceAccount:loader@my-project.iam.gserviceaccount.com",
			"deleted:serviceAccount:recreated@my-project.iam.gserviceaccount.com?uid=1",
		}},
	}}
	refused := &googleapi.Error{Code: 403, Message: "The caller does not have permission"}
	unavailable := &googleapi.Error{Code: 503, Message: "backend unavailable"}

	t.Run("both sources, kept apart", func(t *testing.T) {
		result, err := platformUsersFromSources(ctx, accounts, nil, policy, nil)
		require.NoError(t, err)
		require.Len(t, result.Sources, 2)
		assert.Equal(t, platformUserSourceServiceAccounts, result.Sources[0].Source, "service accounts come first in trust order")
		assert.Equal(t, platformUserSourceIamPolicy, result.Sources[1].Source)

		policyLoader := find(result.Source(platformUserSourceIamPolicy), "loader@my-project.iam.gserviceaccount.com")
		require.NotNil(t, policyLoader)
		assert.Empty(t, policyLoader.PlatformId, "the policy source is not filled from the service accounts")
		assert.Empty(t, policyLoader.DisplayName)
		assert.Nil(t, find(result.Source(platformUserSourceServiceAccounts), "jdoe@example.com"))

		reconciled := result.Reconcile()
		require.Len(t, reconciled.Users, 4)
		loader := find(reconciled, "loader@my-project.iam.gserviceaccount.com")
		assert.Equal(t, "1001", loader.PlatformId)
		assert.Equal(t, "Loader", loader.DisplayName)
		assert.Equal(t, []string{"roles/bigquery.user"}, loader.Roles)
		recreated := find(reconciled, "recreated@my-project.iam.gserviceaccount.com")
		require.NotNil(t, recreated.Disabled)
		assert.False(t, *recreated.Disabled, "the live account the service account list states wins over the policy's deleted binding")
		assert.Equal(t, scrapper.PlatformUsersUnknown, reconciled.Completeness)
		assert.False(t, reconciled.IsSkipped(scrapper.PlatformUserFactRoles))
		assert.False(t, reconciled.IsSkipped(scrapper.PlatformUserFactPlatformId))
		scrappertest.AssertSkipped(t, reconciled, scrapper.PlatformUserFactCreatedAt, scrapper.PlatformUserSkipUnavailable)
	})

	t.Run("service accounts refused", func(t *testing.T) {
		result, err := platformUsersFromSources(ctx, nil, refused, policy, nil)
		require.NoError(t, err)
		require.Len(t, result.Sources, 2)
		sa := result.Source(platformUserSourceServiceAccounts)
		assert.NotEmpty(t, sa.Refused)
		assert.Empty(t, sa.Users)
		assert.Len(t, result.Source(platformUserSourceIamPolicy).Users, 3)
		// A grant on the service account list would state them.
		reconciled := result.Reconcile()
		scrappertest.AssertSkipped(t, reconciled, scrapper.PlatformUserFactPlatformId, scrapper.PlatformUserSkipRefused)
		scrappertest.AssertSkipped(t, reconciled, scrapper.PlatformUserFactDisplayName, scrapper.PlatformUserSkipRefused)
		scrappertest.AssertSkipped(t, reconciled, scrapper.PlatformUserFactCreatedAt, scrapper.PlatformUserSkipUnavailable)
	})

	t.Run("policy refused", func(t *testing.T) {
		result, err := platformUsersFromSources(ctx, accounts, nil, nil, refused)
		require.NoError(t, err)
		assert.NotEmpty(t, result.Source(platformUserSourceIamPolicy).Refused)
		assert.Len(t, result.Source(platformUserSourceServiceAccounts).Users, 3)
		assert.Contains(t, result.Reconcile().CompletenessReason, platformUserSourceIamPolicy)
		// Roles are only in the policy, so a grant on it would state them.
		scrappertest.AssertSkipped(t, result.Reconcile(), scrapper.PlatformUserFactRoles, scrapper.PlatformUserSkipRefused)
	})

	t.Run("both refused is a result, not an error", func(t *testing.T) {
		result, err := platformUsersFromSources(ctx, nil, refused, nil, refused)
		require.NoError(t, err)
		require.Len(t, result.Sources, 2)
		assert.False(t, result.Answered())
		reconciled := result.Reconcile()
		assert.NotEmpty(t, reconciled.Refused)
		assert.Empty(t, reconciled.Users)
	})

	t.Run("one failed, the other answered, is a result", func(t *testing.T) {
		result, err := platformUsersFromSources(ctx, nil, unavailable, policy, nil)
		require.NoError(t, err)
		assert.NotEmpty(t, result.Source(platformUserSourceServiceAccounts).Failed)
		assert.Empty(t, result.Source(platformUserSourceServiceAccounts).Refused)
		assert.Len(t, result.Source(platformUserSourceIamPolicy).Users, 3)
		scrappertest.AssertSkipped(t, result.Reconcile(), scrapper.PlatformUserFactPlatformId, scrapper.PlatformUserSkipFailed)

		result, err = platformUsersFromSources(ctx, accounts, nil, nil, unavailable)
		require.NoError(t, err)
		assert.NotEmpty(t, result.Source(platformUserSourceIamPolicy).Failed)
		scrappertest.AssertSkipped(t, result.Reconcile(), scrapper.PlatformUserFactRoles, scrapper.PlatformUserSkipFailed)
	})

	t.Run("both failed is an error", func(t *testing.T) {
		result, err := platformUsersFromSources(ctx, nil, unavailable, nil, unavailable)
		require.Error(t, err)
		assert.Nil(t, result)
		assert.ErrorIs(t, err, unavailable)
	})

	t.Run("one refused, one failed is an error", func(t *testing.T) {
		_, err := platformUsersFromSources(ctx, nil, refused, nil, unavailable)
		require.Error(t, err, "nothing answered and something failed outright")
	})

	t.Run("a done context is an error", func(t *testing.T) {
		done, cancel := context.WithCancel(ctx)
		cancel()
		_, err := platformUsersFromSources(done, accounts, nil, policy, nil)
		require.ErrorIs(t, err, context.Canceled)
	})
}

func TestPlatformUserSourceError(t *testing.T) {
	assert.Nil(t, platformUserSourceError(platformUserSourceIamPolicy, nil))

	disabled := &googleapi.Error{
		Code:    403,
		Message: "Cloud Resource Manager API has not been used in project 123 before or it is disabled.",
		Details: []interface{}{map[string]interface{}{
			"@type":  "type.googleapis.com/google.rpc.ErrorInfo",
			"reason": "SERVICE_DISABLED",
			"metadata": map[string]interface{}{
				"service":       "cloudresourcemanager.googleapis.com",
				"activationUrl": "https://console.developers.google.com/apis/api/cloudresourcemanager.googleapis.com/overview?project=123",
			},
		}},
	}
	src := platformUserSourceError(platformUserSourceIamPolicy, errors.Wrap(disabled, "reading the IAM policy"))
	assert.Empty(t, src.Refused, "a disabled API is not a grant the role lacks")
	assert.Empty(t, src.Failed)
	assert.Contains(t, src.Unavailable, "cloudresourcemanager.googleapis.com is not enabled")
	assert.Contains(t, src.Unavailable, "https://console.developers.google.com/apis/api/cloudresourcemanager.googleapis.com")
	assert.Equal(t, scrapper.PlatformUserSourceAPI, src.Kind)

	src = platformUserSourceError(platformUserSourceServiceAccounts,
		&googleapi.Error{Code: 403, Message: "Permission 'iam.serviceAccounts.list' denied"})
	assert.NotEmpty(t, src.Refused)
	assert.Empty(t, src.Unavailable)

	src = platformUserSourceError(platformUserSourceServiceAccounts, &googleapi.Error{Code: 500, Message: "internal"})
	assert.NotEmpty(t, src.Failed)
	assert.Empty(t, src.Refused)

	src = platformUserSourceError(platformUserSourceServiceAccounts, context.DeadlineExceeded)
	assert.NotEmpty(t, src.Failed)
}

// TestAccountFactsSkipKind: the policy source skips the service account facts
// the way the service account list did not answer, so a disabled IAM API is
// no grant to recommend.
func TestAccountFactsSkipKind(t *testing.T) {
	policy := &cloudresourcemanager.Policy{Bindings: []*cloudresourcemanager.Binding{
		{Role: "roles/bigquery.user", Members: []string{"user:jdoe@example.com"}},
	}}
	disabled := &googleapi.Error{
		Code: 403,
		Details: []interface{}{map[string]interface{}{
			"@type":  "type.googleapis.com/google.rpc.ErrorInfo",
			"reason": "SERVICE_DISABLED",
		}},
	}
	for _, tc := range []struct {
		name string
		err  error
		want scrapper.PlatformUserSkipKind
	}{
		{"answered", nil, scrapper.PlatformUserSkipUnavailable},
		{"refused", &googleapi.Error{Code: 403, Message: "Permission 'iam.serviceAccounts.list' denied"}, scrapper.PlatformUserSkipRefused},
		{"api disabled", disabled, scrapper.PlatformUserSkipUnavailable},
		{"failed", &googleapi.Error{Code: 500, Message: "internal"}, scrapper.PlatformUserSkipFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var accounts []*iam.ServiceAccount
			if tc.err == nil {
				accounts = []*iam.ServiceAccount{{Email: "loader@my-project.iam.gserviceaccount.com"}}
			}
			result, err := platformUsersFromSources(context.Background(), accounts, tc.err, policy, nil)
			require.NoError(t, err)
			policySource := result.Source(platformUserSourceIamPolicy)
			for _, fact := range []scrapper.PlatformUserFact{
				scrapper.PlatformUserFactPlatformId, scrapper.PlatformUserFactDisplayName,
				scrapper.PlatformUserFactComment, scrapper.PlatformUserFactDisabled,
			} {
				scrappertest.AssertSkipped(t, policySource, fact, tc.want)
			}
			scrappertest.AssertSkipped(t, policySource, scrapper.PlatformUserFactCreatedAt, scrapper.PlatformUserSkipUnavailable)
		})
	}
}

func find(users *scrapper.PlatformUserListing, login string) *scrapper.PlatformUser {
	for _, u := range users.Users {
		if u.Login == login {
			return u
		}
	}
	return nil
}

// TestPlatformUsersGrantNamesEachRefusedPermission: the two calls are refused
// as "Permission 'iam.serviceAccounts.list' denied" and a bare 403 on
// getIamPolicy, and a customer adds what the grant names to the custom role the
// setup guide created, so it names both permissions rather than only a
// predefined role.
func TestPlatformUsersGrantNamesEachRefusedPermission(t *testing.T) {
	assert.Contains(t, platformUsersGrant, "resourcemanager.projects.getIamPolicy")
	assert.Contains(t, platformUsersGrant, "iam.serviceAccounts.list")
	assert.Contains(t, platformUsersGrant, "roles/iam.securityReviewer")
}
