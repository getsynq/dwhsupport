package databricks

import (
	"context"
	"encoding/json"
	"io"
	"net/http"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/config"
	"github.com/databricks/databricks-sdk-go/config/credentials"
	"github.com/databricks/databricks-sdk-go/config/experimental/auth"
	"github.com/databricks/databricks-sdk-go/credentials/u2m"
	"github.com/pkg/errors"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

// m2mCredentialsName is the name the SDK gives its own OAuth M2M strategy. Keeping it
// keeps the config's AuthType, and the auth type the user agent reports, what they
// were.
const m2mCredentialsName = "oauth-m2m"

// withM2mRenewal installs m2mCredentials on a config that authenticates with OAuth
// M2M and has not chosen a credentials strategy of its own. A config with both a
// client id and secret would get the SDK's M2M strategy from its default chain, so
// nothing else changes for it.
func withM2mRenewal(cfg *databricks.Config) {
	if cfg.Credentials != nil || cfg.ClientID == "" || cfg.ClientSecret == "" || cfg.Token != "" {
		return
	}
	if cfg.AuthType != "" && cfg.AuthType != m2mCredentialsName {
		return
	}
	cfg.Credentials = m2mCredentials{}
}

// m2mCredentials is the SDK's OAuth M2M strategy (config.M2mCredentials) with one
// difference: the token is renewed while it is still good for a while.
//
// The SDK's strategy caches the token in a token source that renews it in the
// background once it enters its last stretch (half its lifetime, at most twenty
// minutes). But what that cache asks for a new token is the client-credentials token
// source, which keeps handing back the token it has until ten seconds before it
// expires. So the early renewal gets the same token back, and a new one is only issued
// in the last ten seconds of the old one's life. Every request the pacing holds back
// for longer than that, after the SDK stamped it, reaches the workspace with a token
// that has expired. Here the cache asks the token endpoint directly, so its renewal
// gets a new token.
type m2mCredentials struct{}

func (m2mCredentials) Name() string {
	return m2mCredentialsName
}

func (m2mCredentials) Configure(ctx context.Context, cfg *config.Config) (credentials.CredentialsProvider, error) {
	if cfg.ClientID == "" || cfg.ClientSecret == "" {
		return nil, nil
	}
	endpoints, err := oidcEndpoints(ctx, cfg)
	if err != nil {
		return nil, errors.Wrap(err, "oidc")
	}
	tokenConfig := &clientcredentials.Config{
		ClientID:     cfg.ClientID,
		ClientSecret: cfg.ClientSecret,
		AuthStyle:    oauth2.AuthStyleInHeader,
		TokenURL:     endpoints.TokenEndpoint,
		Scopes:       cfg.GetScopes(),
	}
	// Each call is a request to the token endpoint. ctx is the one the SDK configures
	// credentials with, which carries its own client for token requests, so it is used
	// for every renewal the way the SDK's strategy uses it: the context of whichever
	// request happens to trigger a renewal is not what the renewal should die with.
	fetch := auth.TokenSourceFn(func(context.Context) (*oauth2.Token, error) {
		return tokenConfig.Token(ctx)
	})
	// The SDK's wrapper puts the cache in front, with the config's choice of
	// background or blocking renewal (DisableAsyncTokenRefresh).
	return config.NewTokenSourceStrategy(m2mCredentialsName, fetch).Configure(ctx, cfg)
}

// oidcEndpoints finds the token endpoint the way the SDK does for its M2M strategy
// (Config.getOidcEndpoints, which is not exported): from the configured discovery URL,
// else from the well-known path of the kind of host the config points at. The request
// goes through the client the SDK put in ctx for token requests.
func oidcEndpoints(ctx context.Context, cfg *config.Config) (*u2m.OAuthAuthorizationServer, error) {
	if cfg.DiscoveryURL != "" {
		return fetchOidcEndpoints(ctx, cfg.DiscoveryURL)
	}
	host := cfg.CanonicalHostName()
	switch cfg.HostType() {
	case config.AccountHost:
		return &u2m.OAuthAuthorizationServer{
			AuthorizationEndpoint: host + "/oidc/accounts/" + cfg.AccountID + "/v1/authorize",
			TokenEndpoint:         host + "/oidc/accounts/" + cfg.AccountID + "/v1/token",
		}, nil
	case config.UnifiedHost:
		return fetchOidcEndpoints(ctx, host+"/oidc/accounts/"+cfg.AccountID+"/.well-known/oauth-authorization-server")
	case config.WorkspaceHost:
		return fetchOidcEndpoints(ctx, host+"/oidc/.well-known/oauth-authorization-server")
	default:
		return nil, errors.Errorf("unknown host type: %v", cfg.HostType())
	}
}

func fetchOidcEndpoints(ctx context.Context, discoveryUrl string) (*u2m.OAuthAuthorizationServer, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, discoveryUrl, nil)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	resp, err := oauth2.NewClient(ctx, nil).Do(req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get OAuth endpoints")
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, u2m.ErrOAuthNotSupported
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxPeekedErrorBody))
		return nil, errors.Errorf("failed to get OAuth endpoints from %s: HTTP %d: %s", discoveryUrl, resp.StatusCode, body)
	}
	var endpoints u2m.OAuthAuthorizationServer
	if err := json.NewDecoder(resp.Body).Decode(&endpoints); err != nil {
		return nil, errors.Wrap(err, "failed to decode OAuth endpoints")
	}
	return &endpoints, nil
}
