package bigquery

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/googleapi"
	iamcredentials "google.golang.org/api/iamcredentials/v1"
	"google.golang.org/api/option"
)

// cloudPlatformScope is what every hop but the last asks for: the token is only used to call
// iamcredentials for the next hop.
const cloudPlatformScope = "https://www.googleapis.com/auth/cloud-platform"

// iamCredentialsEndpoint overrides the IAM Service Account Credentials API endpoint. Only tests set it.
var iamCredentialsEndpoint string

// ImpersonationError is a refused or failed token request for ServiceAccount, one hop of
// BigQueryConf.ImpersonateServiceAccounts. Caller is the identity that asked for it: the process's
// own default credentials for the first hop ("" then), the previous service account after that.
type ImpersonationError struct {
	ServiceAccount string
	Caller         string
	Err            error
}

func (e *ImpersonationError) Error() string {
	caller := e.Caller
	if caller == "" {
		caller = "the default credentials"
	}
	return fmt.Sprintf("bigquery: %s cannot impersonate %s: %v", caller, e.ServiceAccount, e.Err)
}

func (e *ImpersonationError) Unwrap() error { return e.Err }

// Denied reports whether Google refused the hop: the caller lacks roles/iam.serviceAccountTokenCreator
// on the service account (403), or the service account does not exist or is disabled (404, 400).
func (e *ImpersonationError) Denied() bool {
	var apiErr *googleapi.Error
	if !errors.As(e.Err, &apiErr) {
		return false
	}
	return apiErr.Code == http.StatusForbidden || apiErr.Code == http.StatusNotFound || apiErr.Code == http.StatusBadRequest
}

// AsImpersonationError returns the impersonation hop err failed on, if it failed on one.
func AsImpersonationError(err error) (*ImpersonationError, bool) {
	var impErr *ImpersonationError
	if errors.As(err, &impErr) {
		return impErr, true
	}
	return nil, false
}

// impersonationTokenSource returns tokens for the last service account of chain, each hop
// authenticated by the token of the hop before it and the first by base. Every hop caches its
// token until shortly before it expires, and gives up on a token request after timeout.
func impersonationTokenSource(
	ctx context.Context,
	base oauth2.TokenSource,
	chain []string,
	scopes []string,
	timeout time.Duration,
) (oauth2.TokenSource, error) {
	if len(chain) == 0 {
		return nil, errors.New("bigquery: no service account to impersonate")
	}
	if len(scopes) == 0 {
		scopes = []string{cloudPlatformScope}
	}
	current := base
	caller := ""
	for i, serviceAccount := range chain {
		if serviceAccount == "" {
			return nil, errors.New("bigquery: empty service account in the impersonation chain")
		}
		hopScopes := []string{cloudPlatformScope}
		if i == len(chain)-1 {
			hopScopes = scopes
		}
		opts := []option.ClientOption{option.WithTokenSource(current)}
		if iamCredentialsEndpoint != "" {
			opts = append(opts, option.WithEndpoint(iamCredentialsEndpoint))
		}
		service, err := iamcredentials.NewService(ctx, opts...)
		if err != nil {
			return nil, errors.Wrap(err, "bigquery: creating the IAM credentials client")
		}
		current = oauth2.ReuseTokenSource(nil, &impersonatedTokenSource{
			ctx:            ctx,
			service:        service,
			serviceAccount: serviceAccount,
			caller:         caller,
			scopes:         hopScopes,
			timeout:        timeout,
		})
		caller = serviceAccount
	}
	return current, nil
}

type impersonatedTokenSource struct {
	ctx            context.Context
	service        *iamcredentials.Service
	serviceAccount string
	caller         string
	scopes         []string
	timeout        time.Duration
}

func (s *impersonatedTokenSource) Token() (*oauth2.Token, error) {
	// Token takes no context, and the timeout of the HTTP client that asks for the token does not
	// reach this request, so it needs its own. ReuseTokenSource holds a lock while it waits, so a
	// request that never ends blocks every call that needs this token.
	ctx, cancel := context.WithTimeout(s.ctx, s.timeout)
	defer cancel()
	name := "projects/-/serviceAccounts/" + s.serviceAccount
	resp, err := s.service.Projects.ServiceAccounts.
		GenerateAccessToken(name, &iamcredentials.GenerateAccessTokenRequest{Scope: s.scopes}).
		Context(ctx).
		Do()
	if err != nil {
		// A hop before this one failed while authenticating this request: that hop is the one
		// to report.
		if earlier, ok := AsImpersonationError(err); ok {
			return nil, earlier
		}
		return nil, &ImpersonationError{ServiceAccount: s.serviceAccount, Caller: s.caller, Err: err}
	}
	expiry, err := time.Parse(time.RFC3339, resp.ExpireTime)
	if err != nil {
		return nil, &ImpersonationError{
			ServiceAccount: s.serviceAccount,
			Caller:         s.caller,
			Err:            errors.Wrap(err, "parsing the token expiry"),
		}
	}
	return &oauth2.Token{AccessToken: resp.AccessToken, TokenType: "Bearer", Expiry: expiry}, nil
}

// impersonationOption authenticates as the last service account of conf.ImpersonateServiceAccounts,
// starting from the process's own default credentials.
func impersonationOption(ctx context.Context, conf *BigQueryConf, scopes []string) (option.ClientOption, error) {
	if conf.CredentialsJson != "" || conf.CredentialsFile != "" {
		return nil, errors.New("bigquery: set either a service account key or service accounts to impersonate, not both")
	}
	// The client outlives the call that builds it and refreshes its tokens later, so the token
	// sources must not stop when ctx is canceled.
	ctx = context.WithoutCancel(ctx)
	base, err := google.DefaultTokenSource(ctx, cloudPlatformScope)
	if err != nil {
		return nil, errors.Wrap(err, "bigquery: finding the default credentials to impersonate with")
	}
	tokenSource, err := impersonationTokenSource(ctx, base, conf.ImpersonateServiceAccounts, scopes, httpTimeout(conf))
	if err != nil {
		return nil, err
	}
	return option.WithTokenSource(tokenSource), nil
}
