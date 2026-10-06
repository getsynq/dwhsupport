package databricks

import (
	"context"
	"slices"

	"github.com/databricks/databricks-sdk-go/listing"
	"github.com/databricks/databricks-sdk-go/service/iam"
	dwhexecdatabricks "github.com/getsynq/dwhsupport/exec/databricks"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
)

// platformUsersGrant is what lets the integration's principal read every user and service
// principal of the workspace through SCIM.
const platformUsersGrant = "membership of the workspace admins group, which may list users and service principals through SCIM " +
	"(GET /api/2.0/preview/scim/v2/Users and /ServicePrincipals)"

// scimPageSize is how many principals one SCIM page asks for. The SDK's own default is
// 10000, far more than one response should carry.
const scimPageSize = 500

// Only the attributes the listing reads, so a large workspace does not ship every
// principal's entitlements and roles.
const (
	scimUserAttributes             = "id,userName,displayName,emails,active,groups"
	scimServicePrincipalAttributes = "id,applicationId,displayName,active,groups"
)

// QueryPlatformUsers lists the workspace's users and service principals through SCIM, on
// the same paced workspace client every other REST call uses.
//
// A user's Login is its userName (an email), a service principal's is its application id:
// both are what the Query History API reports as the user_name of a query they ran. Roles
// are the workspace groups the principal is a direct member of. SCIM states neither when a
// principal was created nor when it last signed in, and Databricks has no default role.
//
// A refused user listing is a permission error. A refused service principal listing keeps
// the users and marks the listing limited, since the service principals are exactly the
// logins a caller most wants to recognise. A rate-limited request fails the call whatever it
// was listing: a listing missing whatever the quota cut off would read as principals that
// were removed.
func (e *DatabricksScrapper) QueryPlatformUsers(ctx context.Context) (*scrapper.PlatformUsers, error) {
	users := &scrapper.PlatformUserListing{
		Source:       "databricks.scim",
		Kind:         scrapper.PlatformUserSourceAPI,
		Completeness: scrapper.PlatformUsersComplete,
	}
	users.Skip(scrapper.PlatformUserFactCreatedAt, "Databricks SCIM does not state when a principal was created")
	users.Skip(scrapper.PlatformUserFactLastLoginAt, "Databricks SCIM does not state when a principal last signed in")
	users.Skip(scrapper.PlatformUserFactDefaultRole, "Databricks has no default role")
	users.Skip(scrapper.PlatformUserFactComment, "Databricks principals carry no comment")

	workspaceUsers, err := listAllScim(ctx, e.client.UsersV2.List(ctx, iam.ListUsersRequest{
		Attributes: scimUserAttributes,
		Count:      scimPageSize,
	}))
	if err != nil {
		return nil, errors.Wrap(err, "failed to list Databricks workspace users")
	}
	for _, u := range workspaceUsers {
		users.Users = append(users.Users, &scrapper.PlatformUser{
			Login:       u.UserName,
			PlatformId:  u.Id,
			Type:        scrapper.PlatformUserTypeDatabricksUser,
			Email:       primaryEmail(u.Emails),
			DisplayName: u.DisplayName,
			Disabled:    scimDisabled(u.Active, u.ForceSendFields),
			Roles:       directGroups(u.Groups),
		})
	}

	principals, err := listAllScim(ctx, e.client.ServicePrincipalsV2.List(ctx, iam.ListServicePrincipalsRequest{
		Attributes: scimServicePrincipalAttributes,
		Count:      scimPageSize,
	}))
	switch {
	case err == nil:
		for _, p := range principals {
			users.Users = append(users.Users, &scrapper.PlatformUser{
				Login:       p.ApplicationId,
				PlatformId:  p.Id,
				Type:        scrapper.PlatformUserTypeDatabricksServicePrincipal,
				DisplayName: p.DisplayName,
				Disabled:    scimDisabled(p.Active, p.ForceSendFields),
				Roles:       directGroups(p.Groups),
			})
		}
	case dwhexecdatabricks.IsPermissionError(err) && !dwhexecdatabricks.IsRateLimitError(err):
		logging.GetLogger(ctx).WithError(err).Warn("Databricks refused the service principal listing, listing users only")
		users.Completeness = scrapper.PlatformUsersLimited
		users.CompletenessReason = "the workspace refused the service principal listing, so only users are listed; " +
			"service principals are listed for " + platformUsersGrant
	default:
		return nil, errors.Wrap(err, "failed to list Databricks service principals")
	}

	return scrapper.NewPlatformUsers(users), nil
}

// listAllScim drains a SCIM listing. The SDK's ListAll stops after Count items, which it
// also sends as the page size, so asking for pages of 500 that way silently returns the
// first 500 principals of a larger workspace.
func listAllScim[T any](ctx context.Context, it listing.Iterator[T]) ([]T, error) {
	return listing.ToSlice(ctx, it)
}

func primaryEmail(emails []iam.ComplexValue) string {
	for _, e := range emails {
		if e.Primary && e.Value != "" {
			return e.Value
		}
	}
	for _, e := range emails {
		if e.Value != "" {
			return e.Value
		}
	}
	return ""
}

// scimDisabled reads SCIM's active flag. The SDK decodes a missing flag and active=false
// alike, and only records in ForceSendFields that the response carried the field, so a
// principal whose response left it out stays unknown rather than disabled.
func scimDisabled(active bool, present []string) *bool {
	if !active && !slices.Contains(present, "Active") {
		return nil
	}
	disabled := !active
	return &disabled
}

// directGroups returns the names of the groups a principal is a member of itself. SCIM also
// lists the groups it belongs to through a nested group, typed "indirect".
func directGroups(groups []iam.ComplexValue) []string {
	var names []string
	for _, g := range groups {
		if g.Type == "indirect" || g.Display == "" {
			continue
		}
		names = append(names, g.Display)
	}
	return names
}
