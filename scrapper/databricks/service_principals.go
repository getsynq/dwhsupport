package databricks

import (
	"context"
	"fmt"

	"github.com/databricks/databricks-sdk-go/service/iam"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/google/uuid"
)

// servicePrincipalLookup returns the display name of the service principal with the given
// application id, or "" when the workspace has none.
type servicePrincipalLookup func(ctx context.Context, applicationId string) (string, error)

// servicePrincipalNames resolves the application ids the Query History API reports as the
// user_name of a query a service principal ran. Each id is looked up once per fetch.
//
// Resolution is best effort: a workspace can refuse the integration's credentials the SCIM
// listing while still serving the query history, so the first failed lookup turns resolution
// off for the rest of the fetch and every query keeps its application id as the only name.
type servicePrincipalNames struct {
	lookup   servicePrincipalLookup
	names    map[string]string
	disabled bool
}

func newServicePrincipalNames(lookup servicePrincipalLookup) *servicePrincipalNames {
	return &servicePrincipalNames{lookup: lookup, names: map[string]string{}}
}

// DisplayName returns the service principal's display name for a user_name that is an
// application id, and "" for anything else, including a human's login.
func (n *servicePrincipalNames) DisplayName(ctx context.Context, userName string) string {
	if _, err := uuid.Parse(userName); err != nil {
		return ""
	}
	if name, ok := n.names[userName]; ok {
		return name
	}
	if n.disabled {
		return ""
	}

	name, err := n.lookup(ctx, userName)
	if err != nil {
		n.disabled = true
		logging.GetLogger(ctx).WithError(err).
			Warn("unable to resolve Databricks service principal names, query logs keep the application id")
		return ""
	}
	n.names[userName] = name
	return name
}

func workspaceServicePrincipalLookup(s *DatabricksScrapper) servicePrincipalLookup {
	return func(ctx context.Context, applicationId string) (string, error) {
		// An application id names one service principal, so the first match ends the listing.
		principals, err := s.client.ServicePrincipalsV2.ListAll(ctx, iam.ListServicePrincipalsRequest{
			Attributes: "applicationId,displayName",
			Filter:     fmt.Sprintf("applicationId eq %q", applicationId),
			Count:      1,
		})
		if err != nil {
			return "", err
		}
		for _, principal := range principals {
			if principal.ApplicationId == applicationId {
				return principal.DisplayName, nil
			}
		}
		return "", nil
	}
}
