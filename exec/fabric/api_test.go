package fabric

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type staticToken struct{ scopes []string }

func (s *staticToken) GetToken(_ context.Context, opts policy.TokenRequestOptions) (azcore.AccessToken, error) {
	s.scopes = opts.Scopes
	return azcore.AccessToken{Token: "test-token", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

func TestListWorkspaceRoleAssignments(t *testing.T) {
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.RequestURI())
		assert.Equal(t, "Bearer test-token", r.Header.Get("Authorization"))
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("continuationToken") == "" {
			_, _ = w.Write([]byte(`{"value":[{"id":"a1","principal":{"id":"u1","displayName":"Jane","type":"User",
				"userDetails":{"userPrincipalName":"jane@example.com"}},"role":"Admin"}],"continuationToken":"next page"}`))
			return
		}
		_, _ = w.Write([]byte(`{"value":[{"id":"a2","principal":{"id":"sp1","displayName":"loader","type":"ServicePrincipal",
			"servicePrincipalDetails":{"aadAppId":"app-1"}},"role":"Contributor"}]}`))
	}))
	defer srv.Close()

	cred := &staticToken{}
	assignments, err := NewAPIClientWith(cred, srv.URL, nil).ListWorkspaceRoleAssignments(context.Background(), "ws-1")
	require.NoError(t, err)
	assert.Equal(t, []string{fabricAPIScope}, cred.scopes)
	assert.Equal(t, []string{"/workspaces/ws-1/roleAssignments", "/workspaces/ws-1/roleAssignments?continuationToken=next+page"}, paths)
	require.Len(t, assignments, 2)
	assert.Equal(t, "jane@example.com", assignments[0].Principal.UserDetails.UserPrincipalName)
	assert.Equal(t, "Admin", assignments[0].Role)
	assert.Equal(t, "app-1", assignments[1].Principal.ServicePrincipalDetails.AadAppID)
}

func TestListWorkspaceRoleAssignmentsRefused(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"errorCode":"InsufficientWorkspaceRole","message":"Workspace role is not sufficient to perform the operation"}`))
	}))
	defer srv.Close()

	_, err := NewAPIClientWith(&staticToken{}, srv.URL, nil).ListWorkspaceRoleAssignments(context.Background(), "ws-1")
	require.Error(t, err)
	var apiErr *APIError
	require.True(t, errors.As(err, &apiErr))
	assert.Equal(t, http.StatusForbidden, apiErr.StatusCode)
	assert.Equal(t, "InsufficientWorkspaceRole", apiErr.ErrorCode)
	assert.True(t, IsPermissionError(err))
}

func TestListWorkspaceRoleAssignmentsServerError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`not json`))
	}))
	defer srv.Close()

	_, err := NewAPIClientWith(&staticToken{}, srv.URL, nil).ListWorkspaceRoleAssignments(context.Background(), "ws-1")
	require.Error(t, err)
	assert.False(t, IsPermissionError(err), "a failing API is not a refusal")
}

func TestAPICredential(t *testing.T) {
	_, err := (&FabricConf{AccessToken: "sql-token", ClientID: "c", ClientSecret: "s"}).apiCredential()
	assert.ErrorIs(t, err, ErrNoAPICredential, "a SQL token cannot call the Fabric API")

	_, err = (&FabricConf{}).apiCredential()
	assert.ErrorIs(t, err, ErrNoAPICredential, "no service principal secret")

	_, err = (&FabricConf{ClientID: "c", ClientSecret: "s", Host: "not-a-fabric-host"}).apiCredential()
	assert.Error(t, err, "no tenant configured and none in the host")

	cred, err := (&FabricConf{ClientID: "c", ClientSecret: "s", TenantID: "00000000-aaaa-4bbb-8ccc-000000000001"}).apiCredential()
	require.NoError(t, err)
	assert.NotNil(t, cred)
}
