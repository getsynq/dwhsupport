package fabric

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/pkg/errors"
)

// FabricAPIBaseURL is the Fabric REST API root.
const FabricAPIBaseURL = "https://api.fabric.microsoft.com/v1"

// fabricAPIScope is the token audience of the Fabric REST API. A token for the
// SQL endpoint (https://database.windows.net/.default) is not accepted there.
const fabricAPIScope = "https://api.fabric.microsoft.com/.default"

// ErrNoAPICredential is returned by NewAPIClient when the configuration can
// only reach the SQL endpoint: a pre-acquired AccessToken is scoped to SQL and
// cannot be exchanged for a Fabric API token.
var ErrNoAPICredential = errors.New("the Fabric connection has no credential for the Fabric REST API (a pre-acquired SQL access token)")

// APIClient calls the Fabric REST API with the connection's own identity.
type APIClient struct {
	cred    azcore.TokenCredential
	baseURL string
	http    *http.Client
}

// NewAPIClient builds a Fabric REST API client from the same identity the SQL
// connection uses: the service principal, or the ambient Azure identity of an
// on-prem agent. It returns ErrNoAPICredential when the configuration holds
// only a pre-acquired SQL token.
func NewAPIClient(conf *FabricConf) (*APIClient, error) {
	cred, err := conf.apiCredential()
	if err != nil {
		return nil, err
	}
	return NewAPIClientWith(cred, FabricAPIBaseURL, nil), nil
}

// NewAPIClientWith builds a client against baseURL with an explicit credential
// and HTTP client (nil for a default one); tests point it at a fake server.
func NewAPIClientWith(cred azcore.TokenCredential, baseURL string, httpClient *http.Client) *APIClient {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 60 * time.Second}
	}
	return &APIClient{cred: cred, baseURL: baseURL, http: httpClient}
}

func (c *FabricConf) apiCredential() (azcore.TokenCredential, error) {
	if c.AccessToken != "" {
		return nil, ErrNoAPICredential
	}
	switch CanonicalAuthType(c.AuthType) {
	case AuthTypeAzureCLI:
		return azidentity.NewAzureCLICredential(nil)
	case AuthTypeDefault:
		return azidentity.NewDefaultAzureCredential(nil)
	case AuthTypeManagedIdentity:
		opts := &azidentity.ManagedIdentityCredentialOptions{}
		if c.ClientID != "" {
			opts.ID = azidentity.ClientID(c.ClientID)
		}
		return azidentity.NewManagedIdentityCredential(opts)
	default:
		if c.ClientID == "" || c.ClientSecret == "" {
			return nil, ErrNoAPICredential
		}
		tenant := c.TenantID
		if tenant == "" {
			// The SQL driver learns the tenant from the endpoint; the REST
			// API has no such handshake, so read it from the hostname.
			identity, err := ParseHostIdentity(c.Host)
			if err != nil {
				return nil, errors.Wrap(err, "no TenantID configured and none in the host")
			}
			tenant = identity.TenantID
		}
		return azidentity.NewClientSecretCredential(tenant, c.ClientID, c.ClientSecret, nil)
	}
}

// APIError is a refusal or failure answered by the Fabric REST API.
type APIError struct {
	StatusCode int
	// ErrorCode is the API's own code, e.g. InsufficientWorkspaceRole.
	ErrorCode string
	Message   string
}

func (e *APIError) Error() string {
	return fmt.Sprintf("fabric api: %d %s: %s", e.StatusCode, e.ErrorCode, e.Message)
}

// IsPermissionError reports whether the API refused the identity: a missing
// workspace role (InsufficientWorkspaceRole) or a missing tenant setting both
// answer 403, an identity the API does not accept answers 401.
func (e *APIError) IsPermissionError() bool {
	return e.StatusCode == http.StatusForbidden || e.StatusCode == http.StatusUnauthorized
}

// WorkspacePrincipal is the principal of a workspace role assignment.
type WorkspacePrincipal struct {
	// ID is the Entra object id of the principal.
	ID          string `json:"id"`
	DisplayName string `json:"displayName"`
	// Type is User, Group, ServicePrincipal or ServicePrincipalProfile.
	Type        string `json:"type"`
	UserDetails *struct {
		UserPrincipalName string `json:"userPrincipalName"`
	} `json:"userDetails,omitempty"`
	ServicePrincipalDetails *struct {
		AadAppID string `json:"aadAppId"`
	} `json:"servicePrincipalDetails,omitempty"`
	GroupDetails *struct {
		GroupType string `json:"groupType"`
	} `json:"groupDetails,omitempty"`
}

// WorkspaceRoleAssignment grants a principal a workspace role: Admin, Member,
// Contributor or Viewer.
type WorkspaceRoleAssignment struct {
	ID        string             `json:"id"`
	Principal WorkspacePrincipal `json:"principal"`
	Role      string             `json:"role"`
}

type roleAssignmentsPage struct {
	Value             []*WorkspaceRoleAssignment `json:"value"`
	ContinuationToken string                     `json:"continuationToken"`
}

// ListWorkspaceRoleAssignments lists who holds a role in the workspace. The
// caller needs the Member or Admin role in it; anything less is refused with
// 403 InsufficientWorkspaceRole.
func (c *APIClient) ListWorkspaceRoleAssignments(ctx context.Context, workspaceID string) ([]*WorkspaceRoleAssignment, error) {
	var all []*WorkspaceRoleAssignment
	continuation := ""
	for {
		u := c.baseURL + "/workspaces/" + url.PathEscape(workspaceID) + "/roleAssignments"
		if continuation != "" {
			u += "?continuationToken=" + url.QueryEscape(continuation)
		}
		var page roleAssignmentsPage
		if err := c.get(ctx, u, &page); err != nil {
			return nil, err
		}
		all = append(all, page.Value...)
		if page.ContinuationToken == "" {
			return all, nil
		}
		continuation = page.ContinuationToken
	}
}

func (c *APIClient) get(ctx context.Context, u string, out any) error {
	token, err := c.cred.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{fabricAPIScope}})
	if err != nil {
		return errors.Wrap(err, "failed to get a Fabric API token")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return errors.WithStack(err)
	}
	req.Header.Set("Authorization", "Bearer "+token.Token)
	req.Header.Set("Accept", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return errors.Wrapf(err, "GET %s", u)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return errors.Wrapf(err, "reading GET %s", u)
	}
	if resp.StatusCode != http.StatusOK {
		apiErr := &APIError{StatusCode: resp.StatusCode}
		var payload struct {
			ErrorCode string `json:"errorCode"`
			Message   string `json:"message"`
		}
		if json.Unmarshal(body, &payload) == nil {
			apiErr.ErrorCode, apiErr.Message = payload.ErrorCode, payload.Message
		}
		return errors.WithStack(apiErr)
	}
	return errors.Wrapf(json.Unmarshal(body, out), "decoding GET %s", u)
}
