package bigquery

import (
	"context"
	"net/http"
	"strings"

	dwhexecbigquery "github.com/getsynq/dwhsupport/exec/bigquery"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
	"google.golang.org/api/cloudresourcemanager/v1"
	iam "google.golang.org/api/iam/v1"
	"google.golang.org/api/option"
)

// Sources of BigQuery platform users, in trust order. Both are Google APIs:
// BigQuery has no SQL view of who may use it.
const (
	// platformUserSourceServiceAccounts is the IAM API's list of the
	// project's own service accounts. It is first because it states whether
	// an account is disabled, while the policy states that only for a member
	// it has already deleted: an address bound only as deleted and then
	// recreated is a live login, and this source says so.
	platformUserSourceServiceAccounts = "bigquery.service_accounts"
	// platformUserSourceIamPolicy is the project's IAM policy: the users and
	// service accounts bound to the project, with their roles.
	platformUserSourceIamPolicy = "bigquery.iam_policy"
)

// platformUsersGrant is what lets a role read both sources:
// resourcemanager.projects.getIamPolicy for the policy and
// iam.serviceAccounts.list for the service accounts. The setup guide has the
// integration's service account hold a custom role, so the two permissions
// are named first, as what to add to it; roles/iam.securityReviewer holds
// both, and reads every other IAM policy of the project too.
const platformUsersGrant = "resourcemanager.projects.getIamPolicy (the bound users and their roles) and " +
	"iam.serviceAccounts.list (the project's service accounts) on the project, added to the role of the integration's service account " +
	"(or roles/iam.securityReviewer, which holds both)"

// iamPolicyCompletenessReason says what a project IAM policy leaves out.
// BigQuery has no list of its users: anyone Google authenticates may run a
// job once some policy lets them. The project policy is the one place that
// names principals, and it names only those bound to the project directly.
const iamPolicyCompletenessReason = "the policy names the users and service accounts bound directly to the project; " +
	"principals that reach BigQuery through a Google group, a folder or organization binding, or a dataset-level grant are not in it"

// serviceAccountsCompletenessReason: the service account list is every
// service account of the project, but people and other projects' service
// accounts are logins of this BigQuery too, and it holds none of them.
const serviceAccountsCompletenessReason = "the list holds every service account of the project, " +
	"and no people or service accounts of other projects"

// QueryPlatformUsers reads two sources, kept apart:
//
//   - bigquery.service_accounts, every service account of the project, bound or
//     not (an unbound one can still be granted access on a dataset), with its
//     unique id, display name, description and disabled state;
//   - bigquery.iam_policy, the users and service accounts bound in the
//     project's IAM policy, with the roles bound to each.
//
// Neither fills in the other's facts; Reconcile does that. A source that could
// not be read is a state of the result (see platformUserSourceError), so a missing
// grant or a disabled API never fails the fetch.
func (e *BigQueryScrapper) QueryPlatformUsers(ctx context.Context) (*scrapper.PlatformUsers, error) {
	// The IAM API accepts only the cloud-platform scope: a cloud-platform.read-only
	// token is refused with "insufficient authentication scopes" by
	// serviceAccounts.list. Both calls only read; the role's grants, not the
	// scope, decide what they may see.
	httpClient, err := dwhexecbigquery.NewHTTPClient(ctx, &e.conf.BigQueryConf, iam.CloudPlatformScope)
	if err != nil {
		return nil, err
	}

	accounts, accountsErr := e.listServiceAccounts(ctx, httpClient)
	policy, policyErr := e.getIamPolicy(ctx, httpClient)
	return platformUsersFromSources(ctx, accounts, accountsErr, policy, policyErr)
}

func (e *BigQueryScrapper) listServiceAccounts(ctx context.Context, httpClient *http.Client) ([]*iam.ServiceAccount, error) {
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
		return nil, errors.Wrapf(err, "listing the service accounts of project %s", e.conf.ProjectId)
	}
	return accounts, nil
}

func (e *BigQueryScrapper) getIamPolicy(ctx context.Context, httpClient *http.Client) (*cloudresourcemanager.Policy, error) {
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
	return policy, nil
}

// platformUsersFromSources builds the result from what each source answered.
// It errors only as CollectPlatformUsers does: when neither source answered
// and one of them failed outright.
func platformUsersFromSources(
	ctx context.Context,
	accounts []*iam.ServiceAccount, accountsErr error,
	policy *cloudresourcemanager.Policy, policyErr error,
) (*scrapper.PlatformUsers, error) {
	accountsSource := platformUserSourceError(platformUserSourceServiceAccounts, accountsErr)
	if accountsSource == nil {
		accountsSource = platformUsersFromServiceAccounts(accounts)
	}
	policySource := platformUserSourceError(platformUserSourceIamPolicy, policyErr)
	if policySource == nil {
		policySource = platformUsersFromPolicy(policy, accountsSource.DependentSkipKind())
	}
	return scrapper.CollectPlatformUsers(ctx, accountsSource, policySource)
}

// platformUserSourceError records a source that could not be read, or returns
// nil when err is nil:
//
//   - unavailable when the API it is read through is disabled
//     (SERVICE_DISABLED, a 403 like a refusal). No grant to the role fixes
//     it: a project owner has to enable the API, which may be a deliberate
//     choice, so it reads like a platform without the source rather than a
//     role without a grant. The reason names the API and where to enable it;
//   - refused on any other permission error (IsPermissionError);
//   - failed otherwise (5xx, timeout).
func platformUserSourceError(source string, err error) *scrapper.PlatformUserListing {
	if err == nil {
		return nil
	}
	if info, disabled := serviceDisabledFromErr(err); disabled {
		service := info.service
		if service == "" {
			service = "the Google API this source is read through"
		}
		// Google checks the API in the project the call is billed to (a
		// service account key's own project), which need not be the BigQuery
		// project; the activation URL names the right one.
		reason := errors.Errorf("%s is not enabled in the Google Cloud project the credentials call it from", service)
		if info.activationURL != "" {
			reason = errors.Errorf("%s is not enabled in the Google Cloud project the credentials call it from, enable it at %s",
				service, info.activationURL)
		}
		return scrapper.UnavailablePlatformUserSource(source, scrapper.PlatformUserSourceAPI, reason)
	}
	return scrapper.PlatformUserSourceError(source, scrapper.PlatformUserSourceAPI, err, dwhexecbigquery.IsPermissionError, nil)
}

// platformUsersFromServiceAccounts lists every service account of the
// project. Its email is the login jobs report as user_email.
func platformUsersFromServiceAccounts(accounts []*iam.ServiceAccount) *scrapper.PlatformUserListing {
	users := &scrapper.PlatformUserListing{
		Source:             platformUserSourceServiceAccounts,
		Kind:               scrapper.PlatformUserSourceAPI,
		Completeness:       scrapper.PlatformUsersUnknown,
		CompletenessReason: serviceAccountsCompletenessReason,
	}
	notInIam := "IAM does not record it"
	users.Skip(scrapper.PlatformUserFactCreatedAt, scrapper.PlatformUserSkipUnavailable, notInIam)
	users.Skip(scrapper.PlatformUserFactLastLoginAt, scrapper.PlatformUserSkipUnavailable, notInIam)
	users.Skip(scrapper.PlatformUserFactDefaultRole, scrapper.PlatformUserSkipUnavailable, "BigQuery has no default role")
	users.Skip(
		scrapper.PlatformUserFactRoles,
		scrapper.PlatformUserSkipUnavailable,
		"roles are bound in IAM policies, see "+platformUserSourceIamPolicy,
	)

	for _, a := range accounts {
		if a == nil || a.Email == "" {
			continue
		}
		disabled := a.Disabled
		users.Users = append(users.Users, &scrapper.PlatformUser{
			Login:       a.Email,
			Email:       a.Email,
			Type:        scrapper.PlatformUserTypeBigQueryServiceAccount,
			PlatformId:  a.UniqueId,
			DisplayName: a.DisplayName,
			Comment:     a.Description,
			Disabled:    &disabled,
		})
	}
	return users
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
//
// accountFactsKind is how the facts only the service account list states are
// skipped (see PlatformUserListing.DependentSkipKind).
func platformUsersFromPolicy(policy *cloudresourcemanager.Policy, accountFactsKind scrapper.PlatformUserSkipKind) *scrapper.PlatformUserListing {
	users := &scrapper.PlatformUserListing{
		Source:             platformUserSourceIamPolicy,
		Kind:               scrapper.PlatformUserSourceAPI,
		Completeness:       scrapper.PlatformUsersUnknown,
		CompletenessReason: iamPolicyCompletenessReason,
	}
	notInIam := "IAM does not record it"
	users.Skip(scrapper.PlatformUserFactCreatedAt, scrapper.PlatformUserSkipUnavailable, notInIam)
	users.Skip(scrapper.PlatformUserFactLastLoginAt, scrapper.PlatformUserSkipUnavailable, notInIam)
	users.Skip(scrapper.PlatformUserFactDefaultRole, scrapper.PlatformUserSkipUnavailable, "BigQuery has no default role")
	accountFacts := "a policy names members only, see " + platformUserSourceServiceAccounts
	users.Skip(scrapper.PlatformUserFactPlatformId, accountFactsKind, accountFacts)
	users.Skip(scrapper.PlatformUserFactDisplayName, accountFactsKind, accountFacts)
	users.Skip(scrapper.PlatformUserFactComment, accountFactsKind, accountFacts)
	users.Skip(
		scrapper.PlatformUserFactDisabled,
		accountFactsKind,
		"a policy states it only for a member it has deleted, see "+platformUserSourceServiceAccounts,
	)

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
