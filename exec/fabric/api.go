package fabric

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
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
	// operationTimeout bounds the wait for one long running operation.
	operationTimeout time.Duration
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
	return &APIClient{cred: cred, baseURL: baseURL, http: httpClient, operationTimeout: maxOperationWait}
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

// maxThrottledRetries is how many times one request waits out a 429 before it
// fails. Fabric throttles each principal, and the kernels that read the API share
// the quota of one service principal.
const maxThrottledRetries = 3

// maxOperationWait is how long a long running operation may take before the call
// fails. A definition that Fabric does not give in this time is no answer.
const maxOperationWait = 5 * time.Minute

// maxRetryAfter caps the wait that a 429 or a long running operation asks for, so
// one answer cannot hold a catalog refresh for long.
const maxRetryAfter = 60 * time.Second

// WorkspaceItem is one item of a workspace: a Warehouse, a Lakehouse, a data
// pipeline, a report and so on.
type WorkspaceItem struct {
	// ID is the item GUID, which also names the item's directory in OneLake.
	ID          string `json:"id"`
	Type        string `json:"type"`
	DisplayName string `json:"displayName"`
}

type workspaceItemsPage struct {
	Value             []*WorkspaceItem `json:"value"`
	ContinuationToken string           `json:"continuationToken"`
}

// ListWorkspaceItems lists the items of the workspace. Any workspace role can
// list them.
func (c *APIClient) ListWorkspaceItems(ctx context.Context, workspaceID string) ([]*WorkspaceItem, error) {
	var all []*WorkspaceItem
	continuation := ""
	for {
		u := c.baseURL + "/workspaces/" + url.PathEscape(workspaceID) + "/items"
		if continuation != "" {
			u += "?continuationToken=" + url.QueryEscape(continuation)
		}
		var page workspaceItemsPage
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
	return c.do(ctx, http.MethodGet, u, out)
}

// do sends one request and decodes a 200 answer into out. A 429 is retried after
// its Retry-After, up to maxThrottledRetries times. A 202 is a long running
// operation (getDefinition): do polls its Location until the operation ends, then
// decodes its result into out.
func (c *APIClient) do(ctx context.Context, method, u string, out any) error {
	for attempt := 0; ; attempt++ {
		resp, body, err := c.send(ctx, method, u)
		if err != nil {
			return err
		}
		switch {
		case resp.StatusCode == http.StatusTooManyRequests && attempt < maxThrottledRetries:
			if err := sleep(ctx, retryAfter(resp)); err != nil {
				return err
			}
			continue
		case resp.StatusCode == http.StatusAccepted && resp.Header.Get("Location") != "":
			return c.waitForOperation(ctx, resp, out)
		case resp.StatusCode == http.StatusOK:
			return errors.Wrapf(json.Unmarshal(body, out), "decoding %s %s", method, u)
		}
		return apiErrorOf(resp.StatusCode, body)
	}
}

// operationState is the body of a long running operation.
type operationState struct {
	Status string `json:"status"`
	Error  *struct {
		ErrorCode string `json:"errorCode"`
		Message   string `json:"message"`
	} `json:"error"`
}

// waitForOperation polls a long running operation until it succeeds or fails. A
// succeeded operation has its answer at <Location>/result. Each poll waits for the
// Retry-After of the answer before it. The operation fails after
// c.operationTimeout, and on a status that the client does not know.
func (c *APIClient) waitForOperation(ctx context.Context, accepted *http.Response, out any) error {
	ctx, cancel := context.WithTimeout(ctx, c.operationTimeout)
	defer cancel()
	location := accepted.Header.Get("Location")
	wait := retryAfter(accepted)
	for {
		if err := sleep(ctx, wait); err != nil {
			return errors.Wrap(err, "waiting for a Fabric operation")
		}
		resp, body, err := c.send(ctx, http.MethodGet, location)
		if err != nil {
			return errors.Wrap(err, "polling a Fabric operation")
		}
		wait = retryAfter(resp)
		if resp.StatusCode == http.StatusTooManyRequests {
			continue
		}
		if resp.StatusCode != http.StatusOK {
			return errors.Wrap(apiErrorOf(resp.StatusCode, body), "polling a Fabric operation")
		}
		var state operationState
		if err := json.Unmarshal(body, &state); err != nil {
			return errors.Wrap(err, "decoding a Fabric operation")
		}
		switch state.Status {
		case "NotStarted", "Running":
			continue
		case "Succeeded":
			return c.do(ctx, http.MethodGet, strings.TrimSuffix(location, "/")+"/result", out)
		case "Failed":
			apiErr := &APIError{StatusCode: http.StatusOK, Message: "the operation failed"}
			if state.Error != nil {
				apiErr.ErrorCode, apiErr.Message = state.Error.ErrorCode, state.Error.Message
			}
			return errors.WithStack(apiErr)
		}
		return errors.Errorf("the Fabric operation has the status %q, which the client does not know", state.Status)
	}
}

func (c *APIClient) send(ctx context.Context, method, u string) (*http.Response, []byte, error) {
	token, err := c.cred.GetToken(ctx, policy.TokenRequestOptions{Scopes: []string{fabricAPIScope}})
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to get a Fabric API token")
	}
	req, err := http.NewRequestWithContext(ctx, method, u, nil)
	if err != nil {
		return nil, nil, errors.WithStack(err)
	}
	req.Header.Set("Authorization", "Bearer "+token.Token)
	req.Header.Set("Accept", "application/json")
	if method != http.MethodGet {
		// Fabric answers 411 to a POST with no length.
		req.ContentLength = 0
		req.Header.Set("Content-Length", "0")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "%s %s", method, u)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "reading %s %s", method, u)
	}
	return resp, body, nil
}

// apiErrorOf builds the error of an answer that is not a success.
func apiErrorOf(statusCode int, body []byte) error {
	apiErr := &APIError{StatusCode: statusCode}
	var payload struct {
		ErrorCode string `json:"errorCode"`
		Message   string `json:"message"`
	}
	if json.Unmarshal(body, &payload) == nil {
		apiErr.ErrorCode, apiErr.Message = payload.ErrorCode, payload.Message
	}
	return errors.WithStack(apiErr)
}

// retryAfter is the wait that an answer asks for in seconds, one second if it asks
// for none or for a value that is no number, and at most maxRetryAfter.
func retryAfter(resp *http.Response) time.Duration {
	seconds, err := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After")))
	if err != nil || seconds < 0 {
		return time.Second
	}
	return min(time.Duration(seconds)*time.Second, maxRetryAfter)
}

func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
