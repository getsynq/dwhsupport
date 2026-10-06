package bigquery

import (
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
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

	users := platformUsersFromPolicy(policy).Finish()

	assert.Equal(t, platformUserSourceIamPolicy, users.Source)
	assert.Equal(t, scrapper.PlatformUserSourceAPI, users.Kind)
	assert.Equal(t, scrapper.PlatformUsersUnknown, users.Completeness)
	assert.Equal(t, iamPolicyCompletenessReason, users.CompletenessReason)
	for _, fact := range []scrapper.PlatformUserFact{
		scrapper.PlatformUserFactCreatedAt, scrapper.PlatformUserFactLastLoginAt, scrapper.PlatformUserFactDefaultRole,
		scrapper.PlatformUserFactPlatformId, scrapper.PlatformUserFactDisplayName, scrapper.PlatformUserFactComment,
		scrapper.PlatformUserFactDisabled,
	} {
		assert.Truef(t, users.IsSkipped(fact), "%s", fact)
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
	users := platformUsersFromPolicy(&cloudresourcemanager.Policy{}).Finish()
	assert.Equal(t, scrapper.PlatformUsersEmpty, users.Completeness)
	assert.Empty(t, users.Users)

	users = platformUsersFromPolicy(nil).Finish()
	assert.Equal(t, scrapper.PlatformUsersEmpty, users.Completeness)
}

func TestPlatformUsersFromServiceAccounts(t *testing.T) {
	users := platformUsersFromServiceAccounts([]*iam.ServiceAccount{
		{Email: "loader@my-project.iam.gserviceaccount.com", UniqueId: "1001", DisplayName: "Loader", Description: "Fivetran loads", Disabled: true},
		{Email: "unbound@my-project.iam.gserviceaccount.com", UniqueId: "1002"},
		{Email: ""},
		nil,
	}).Finish()

	assert.Equal(t, platformUserSourceServiceAccounts, users.Source)
	assert.Equal(t, scrapper.PlatformUserSourceAPI, users.Kind)
	assert.Equal(t, scrapper.PlatformUsersUnknown, users.Completeness)
	assert.Equal(t, serviceAccountsCompletenessReason, users.CompletenessReason)
	for _, fact := range []scrapper.PlatformUserFact{
		scrapper.PlatformUserFactCreatedAt, scrapper.PlatformUserFactLastLoginAt,
		scrapper.PlatformUserFactDefaultRole, scrapper.PlatformUserFactRoles,
	} {
		assert.Truef(t, users.IsSkipped(fact), "%s", fact)
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
	accounts := []*iam.ServiceAccount{
		{Email: "loader@my-project.iam.gserviceaccount.com", UniqueId: "1001", DisplayName: "Loader"},
		{Email: "unbound@my-project.iam.gserviceaccount.com", UniqueId: "1002"},
	}
	policy := &cloudresourcemanager.Policy{Bindings: []*cloudresourcemanager.Binding{
		{Role: "roles/bigquery.user", Members: []string{
			"user:jdoe@example.com",
			"serviceAccount:loader@my-project.iam.gserviceaccount.com",
			"deleted:serviceAccount:recreated@my-project.iam.gserviceaccount.com?uid=1",
		}},
	}}
	accounts = append(accounts, &iam.ServiceAccount{Email: "recreated@my-project.iam.gserviceaccount.com", UniqueId: "1003"})
	refused := &googleapi.Error{Code: 403, Message: "The caller does not have permission"}

	t.Run("both sources, kept apart", func(t *testing.T) {
		result, err := platformUsersFromSources(accounts, nil, policy, nil)
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
		assert.True(t, reconciled.IsSkipped(scrapper.PlatformUserFactCreatedAt))
	})

	t.Run("service accounts refused", func(t *testing.T) {
		result, err := platformUsersFromSources(nil, refused, policy, nil)
		require.NoError(t, err)
		require.Len(t, result.Sources, 2)
		sa := result.Source(platformUserSourceServiceAccounts)
		assert.NotEmpty(t, sa.Refused)
		assert.Empty(t, sa.Users)
		assert.Len(t, result.Source(platformUserSourceIamPolicy).Users, 3)
		assert.True(t, result.Reconcile().IsSkipped(scrapper.PlatformUserFactPlatformId))
	})

	t.Run("policy refused", func(t *testing.T) {
		result, err := platformUsersFromSources(accounts, nil, nil, refused)
		require.NoError(t, err)
		assert.NotEmpty(t, result.Source(platformUserSourceIamPolicy).Refused)
		assert.Len(t, result.Source(platformUserSourceServiceAccounts).Users, 3)
		assert.Contains(t, result.Reconcile().CompletenessReason, platformUserSourceIamPolicy+" refused")
	})

	t.Run("both refused is a permission error", func(t *testing.T) {
		result, err := platformUsersFromSources(nil, refused, nil, refused)
		assert.Nil(t, result)
		require.Error(t, err)
		assert.True(t, (&BigQueryScrapper{}).IsPermissionError(err))
	})

	t.Run("an error that is not a refusal fails the call", func(t *testing.T) {
		unavailable := &googleapi.Error{Code: 503, Message: "backend unavailable"}
		_, err := platformUsersFromSources(nil, unavailable, policy, nil)
		require.ErrorIs(t, err, unavailable)
		_, err = platformUsersFromSources(accounts, nil, nil, unavailable)
		require.ErrorIs(t, err, unavailable)
	})
}

func find(users *scrapper.PlatformUserListing, login string) *scrapper.PlatformUser {
	for _, u := range users.Users {
		if u.Login == login {
			return u
		}
	}
	return nil
}
