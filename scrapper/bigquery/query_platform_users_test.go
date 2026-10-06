package bigquery

import (
	"testing"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/cloudresourcemanager/v1"
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

	assert.Equal(t, scrapper.PlatformUsersUnknown, users.Completeness)
	assert.Equal(t, platformUsersCompletenessReason, users.CompletenessReason)
	assert.True(t, users.IsSkipped(scrapper.PlatformUserFactCreatedAt))
	assert.True(t, users.IsSkipped(scrapper.PlatformUserFactLastLoginAt))
	assert.True(t, users.IsSkipped(scrapper.PlatformUserFactDefaultRole))

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

func TestAddServiceAccountDetails(t *testing.T) {
	users := platformUsersFromPolicy(&cloudresourcemanager.Policy{Bindings: []*cloudresourcemanager.Binding{
		{Role: "roles/bigquery.user", Members: []string{
			"user:jdoe@example.com",
			"serviceAccount:loader@my-project.iam.gserviceaccount.com",
			"serviceAccount:etl@other-project.iam.gserviceaccount.com",
			"deleted:serviceAccount:old@my-project.iam.gserviceaccount.com?uid=1",
		}},
	}})
	addServiceAccountDetails(users, []*iam.ServiceAccount{
		{Email: "loader@my-project.iam.gserviceaccount.com", UniqueId: "1001", DisplayName: "Loader", Description: "Fivetran loads", Disabled: true},
		{Email: "unbound@my-project.iam.gserviceaccount.com", UniqueId: "1002"},
		{Email: "old@my-project.iam.gserviceaccount.com", UniqueId: "1003"},
		nil,
	})
	users.Finish()

	require.Len(t, users.Users, 4, "a service account without a project binding is not added")
	byLogin := map[string]*scrapper.PlatformUser{}
	for _, u := range users.Users {
		byLogin[u.Login] = u
	}
	loader := byLogin["loader@my-project.iam.gserviceaccount.com"]
	assert.Equal(t, "1001", loader.PlatformId)
	assert.Equal(t, "Loader", loader.DisplayName)
	assert.Equal(t, "Fivetran loads", loader.Comment)
	require.NotNil(t, loader.Disabled)
	assert.True(t, *loader.Disabled)

	other := byLogin["etl@other-project.iam.gserviceaccount.com"]
	assert.Empty(t, other.PlatformId)
	assert.Nil(t, other.Disabled)

	assert.Empty(t, byLogin["jdoe@example.com"].PlatformId)
	assert.Empty(t, byLogin["old@my-project.iam.gserviceaccount.com"].PlatformId, "a deleted binding is not the live account")
}
