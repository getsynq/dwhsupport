package bigquery

import (
	"context"
	"strings"

	dwhexecbigquery "github.com/getsynq/dwhsupport/exec/bigquery"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
	"google.golang.org/api/cloudresourcemanager/v1"
	iam "google.golang.org/api/iam/v1"
	"google.golang.org/api/option"
)

// platformUsersGrant is what lets a role read the project's IAM policy
// (resourcemanager.projects.getIamPolicy) and the details of the project's
// service accounts (iam.serviceAccounts.list). roles/iam.securityReviewer holds
// both; roles/browser holds only the first.
const platformUsersGrant = "roles/iam.securityReviewer on the project " +
	"(resourcemanager.projects.getIamPolicy to list the users, iam.serviceAccounts.list for service account details)"

// platformUsersCompletenessReason says what a project IAM policy leaves out.
// BigQuery has no list of its users: anyone Google authenticates may run a
// job once some policy lets them. The project policy is the one place that
// names principals, and it names only those bound to the project directly.
const platformUsersCompletenessReason = "the listing holds the users and service accounts bound directly in the project's IAM policy; " +
	"principals that reach BigQuery through a Google group, a folder or organization binding, or a dataset-level grant are not listed"

// QueryPlatformUsers lists the users and service accounts bound in the
// project's IAM policy, each with the roles bound to it there. Service accounts
// of the project itself also get their display name, description, disabled
// state and unique id from the IAM API, when the role may list them.
//
// A refused getIamPolicy is a permission error. A refused service account
// listing only skips the facts it would have added.
func (e *BigQueryScrapper) QueryPlatformUsers(ctx context.Context) (*scrapper.PlatformUsers, error) {
	httpClient, err := dwhexecbigquery.NewHTTPClient(ctx, &e.conf.BigQueryConf, cloudresourcemanager.CloudPlatformReadOnlyScope)
	if err != nil {
		return nil, err
	}

	crm, err := cloudresourcemanager.NewService(ctx, option.WithHTTPClient(httpClient))
	if err != nil {
		return nil, err
	}
	policy, err := crm.Projects.GetIamPolicy(e.conf.ProjectId, &cloudresourcemanager.GetIamPolicyRequest{
		// Version 3 returns conditional bindings as they are instead of
		// refusing the request for a policy that has any.
		Options: &cloudresourcemanager.GetPolicyOptions{RequestedPolicyVersion: 3},
	}).Context(ctx).Do()
	if err != nil {
		return nil, errors.Wrapf(err, "reading the IAM policy of project %s", e.conf.ProjectId)
	}

	users := platformUsersFromPolicy(policy)

	iamService, err := iam.NewService(ctx, option.WithHTTPClient(httpClient))
	if err != nil {
		return nil, err
	}
	var accounts []*iam.ServiceAccount
	err = iamService.Projects.ServiceAccounts.List("projects/"+e.conf.ProjectId).
		Pages(ctx, func(page *iam.ListServiceAccountsResponse) error {
			accounts = append(accounts, page.Accounts...)
			return nil
		})
	if err != nil {
		logging.GetLogger(ctx).WithError(err).Warn("failed to list bigquery project service accounts, continuing without their details")
		reason := "listing the project's service accounts failed: " + err.Error()
		if dwhexecbigquery.IsPermissionError(err) {
			reason = "the role may not list the project's service accounts (iam.serviceAccounts.list, in roles/iam.securityReviewer)"
		}
		users.Skip(scrapper.PlatformUserFactPlatformId, reason)
		users.Skip(scrapper.PlatformUserFactDisplayName, reason)
		users.Skip(scrapper.PlatformUserFactComment, reason)
		users.Skip(scrapper.PlatformUserFactDisabled, reason)
	} else {
		addServiceAccountDetails(users, accounts)
	}

	return users.Finish(), nil
}

// platformUsersFromPolicy maps the members of a project IAM policy to platform
// users, with the roles each is bound to.
//
//   - user: and serviceAccount: members are logins, listed under their address,
//     which is what INFORMATION_SCHEMA.JOBS reports as user_email.
//   - deleted:user: and deleted:serviceAccount: members are logins that no
//     longer exist but are still bound. Query history can still name them, so
//     they are listed as disabled, with the roles they are still bound to,
//     unless the same address is bound live (it was recreated), in which case
//     the deleted binding belongs to a former account and is ignored.
//   - group:, domain:, allUsers, allAuthenticatedUsers and principal sets name
//     who may sign in, not a login, so they are not users. Workforce and
//     workload identity principals (principal://) are not listed either: jobs
//     do not report them under that string.
//
// A conditional binding counts like any other: the role is bound, under a
// condition the listing does not evaluate.
func platformUsersFromPolicy(policy *cloudresourcemanager.Policy) *scrapper.PlatformUsers {
	users := &scrapper.PlatformUsers{
		Completeness:       scrapper.PlatformUsersUnknown,
		CompletenessReason: platformUsersCompletenessReason,
	}
	notInIam := "IAM does not record it"
	users.Skip(scrapper.PlatformUserFactCreatedAt, notInIam)
	users.Skip(scrapper.PlatformUserFactLastLoginAt, notInIam)
	users.Skip(scrapper.PlatformUserFactDefaultRole, "BigQuery has no default role")

	live := map[string]*scrapper.PlatformUser{}
	deleted := map[string]*scrapper.PlatformUser{}
	var order []string
	if policy == nil {
		return users
	}
	for _, binding := range policy.Bindings {
		if binding == nil {
			continue
		}
		for _, member := range binding.Members {
			userType, address, isDeleted, ok := parseIamMember(member)
			if !ok {
				continue
			}
			byAddress := live
			if isDeleted {
				byAddress = deleted
			}
			u, seen := byAddress[address]
			if !seen {
				u = &scrapper.PlatformUser{Login: address, Email: address, Type: userType}
				if isDeleted {
					disabled := true
					u.Disabled = &disabled
				}
				byAddress[address] = u
				order = append(order, address)
			}
			u.Roles = append(u.Roles, binding.Role)
		}
	}
	emitted := map[string]bool{}
	for _, address := range order {
		if emitted[address] {
			continue
		}
		emitted[address] = true
		if u, ok := live[address]; ok {
			users.Users = append(users.Users, u)
		} else {
			users.Users = append(users.Users, deleted[address])
		}
	}
	return users
}

// parseIamMember splits an IAM member into the login type and address. ok is
// false for members that are not a single login.
func parseIamMember(member string) (userType, address string, isDeleted, ok bool) {
	if rest, found := strings.CutPrefix(member, "deleted:"); found {
		member = rest
		isDeleted = true
		// deleted:user:jdoe@example.com?uid=123456789: the uid tells two
		// accounts that shared an address apart, the login is the address.
		if i := strings.IndexByte(member, '?'); i >= 0 {
			member = member[:i]
		}
	}
	kind, address, found := strings.Cut(member, ":")
	if !found || address == "" {
		return "", "", false, false
	}
	switch kind {
	case "user":
		return scrapper.PlatformUserTypeBigQueryUser, address, isDeleted, true
	case "serviceAccount":
		return scrapper.PlatformUserTypeBigQueryServiceAccount, address, isDeleted, true
	}
	return "", "", false, false
}

// addServiceAccountDetails adds what the IAM API states about the project's
// own service accounts to the listed ones. A service account of another
// project bound here keeps only its address and roles.
func addServiceAccountDetails(users *scrapper.PlatformUsers, accounts []*iam.ServiceAccount) {
	byEmail := make(map[string]*iam.ServiceAccount, len(accounts))
	for _, a := range accounts {
		if a != nil {
			byEmail[a.Email] = a
		}
	}
	for _, u := range users.Users {
		if u.Type != scrapper.PlatformUserTypeBigQueryServiceAccount || u.Disabled != nil {
			continue
		}
		a, ok := byEmail[u.Login]
		if !ok {
			continue
		}
		u.PlatformId = a.UniqueId
		u.DisplayName = a.DisplayName
		u.Comment = a.Description
		disabled := a.Disabled
		u.Disabled = &disabled
	}
}
