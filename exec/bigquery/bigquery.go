package bigquery

import (
	"context"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/oauth2"

	"cloud.google.com/go/bigquery"
	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/querycontext"
	"github.com/getsynq/dwhsupport/exec/querystats"
	_ "github.com/lib/pq"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	htransport "google.golang.org/api/transport/http"
)

// defaultHTTPTimeout caps each BigQuery REST round-trip. Without it the Go
// BigQuery client's HTTP transport has no timeout, so a single stalled metadata
// call hangs until the caller's context deadline (e.g. a 2h job timeout),
// wedging the whole concurrent scrape. It is per round-trip, not per logical
// operation, so it is safe for paginated query-result reads while still
// interrupting a blackholed connection.
const defaultHTTPTimeout = 2 * time.Minute

// BigQueryConf configures the BigQuery executor connection.
// Authentication precedence: AccessToken > ImpersonateServiceAccounts > CredentialsJson >
// CredentialsFile > Application Default Credentials, the last only when AllowDefaultCredentials is set.
type BigQueryConf struct {
	// ProjectId is the GCP project containing BigQuery datasets.
	ProjectId string
	// Region restricts queries to a specific BQ region (e.g. "EU", "us-central1").
	Region string
	// CredentialsJson is a GCP service account key in JSON format.
	CredentialsJson string
	// CredentialsFile is a path to a GCP service account key file.
	CredentialsFile string
	// AccessToken is an OAuth access token for user-level authentication.
	// When set, it takes precedence over CredentialsJson and CredentialsFile.
	AccessToken string
	// ImpersonateServiceAccounts is a chain of service account emails. The process's own default
	// credentials get a token for the first one, that one for the next, and BigQuery sees the last.
	// Each one must grant roles/iam.serviceAccountTokenCreator to the identity before it. Google
	// mints the tokens, so no key leaves anyone's project, and the chain needs no
	// AllowDefaultCredentials: whoever builds it decides which service accounts it names. It can not
	// be combined with CredentialsJson or CredentialsFile.
	ImpersonateServiceAccounts []string
	// AllowDefaultCredentials opts into Application Default Credentials (GOOGLE_APPLICATION_CREDENTIALS,
	// gcloud's login, the GKE or GCE metadata server) when no credential is set. Only a process running
	// in the customer's own environment sets it, such as the synq-dwh agent. On the hosted side it
	// stays false, so a connection with no credential is refused instead of running as our own
	// service account.
	AllowDefaultCredentials bool
	// AllowAnyCredentialType accepts every credential type Google's client libraries load in
	// CredentialsJson and CredentialsFile (external_account, impersonated_service_account,
	// authorized_user). Some of them read a file or call a URL that the credential names. Only a
	// process whose configuration comes from the host's owner sets it, such as the synq-dwh agent.
	// When false, the credential must pass ValidateServiceAccountKey.
	AllowAnyCredentialType bool
	// HTTPTimeout caps each BigQuery REST round-trip (table/dataset metadata,
	// table listing, query-result pages). Guards against a stalled connection
	// hanging a scrape until the caller's job deadline. Zero uses
	// defaultHTTPTimeout.
	HTTPTimeout time.Duration
}

type Executor interface {
	QueryRowsIterator(ctx context.Context, q string, args ...interface{}) (*bigquery.RowIterator, error)
}

type BigQueryExecutor struct {
	conf   *BigQueryConf
	client *bigquery.Client
}

// NewHTTPClient returns an HTTP client authenticated the way conf says (see BigQueryConf) for the
// given OAuth scopes, with conf's per-request timeout. The BigQuery client and the scrapper's
// Resource Manager, IAM and Data Catalog calls all go through it, so they act as the same identity.
func NewHTTPClient(ctx context.Context, conf *BigQueryConf, scopes ...string) (*http.Client, error) {
	var options []option.ClientOption
	options = append(options, option.WithUserAgent("synq-bq-client-v1.0.0"))
	options = append(options, option.WithScopes(scopes...))
	if conf.AccessToken != "" {
		tokenSource := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: conf.AccessToken})
		options = append(options, option.WithTokenSource(tokenSource))
	} else if len(conf.ImpersonateServiceAccounts) > 0 {
		credential, err := impersonationOption(ctx, conf, scopes)
		if err != nil {
			return nil, exec.NewAuthError(err)
		}
		options = append(options, credential)
	} else {
		credential, err := credentialOption(conf)
		if err != nil {
			return nil, exec.NewAuthError(err)
		}
		if credential != nil {
			options = append(options, credential)
		}
	}

	// Build the HTTP client ourselves so we can enforce a per-request timeout.
	// htransport.NewClient applies the auth options above AND keeps the
	// google-api OTel HTTP instrumentation; passing a bare &http.Client via
	// option.WithHTTPClient would drop both credentials and tracing.
	httpTimeout := conf.HTTPTimeout
	if httpTimeout <= 0 {
		httpTimeout = defaultHTTPTimeout
	}
	httpClient, _, err := htransport.NewClient(ctx, options...)
	if err != nil {
		return nil, err
	}
	httpClient.Timeout = httpTimeout
	return httpClient, nil
}

func NewBigqueryExecutor(ctx context.Context, conf *BigQueryConf) (*BigQueryExecutor, error) {
	// bigquery.NewClient normally injects this scope before building its
	// transport, but option.WithHTTPClient (below) overrides the transport — so
	// we must apply the scope ourselves or credential-based auth fails with
	// "invalid_scope" when fetching a token.
	httpClient, err := NewHTTPClient(ctx, conf, bigquery.Scope)
	if err != nil {
		return nil, err
	}

	client, err := bigquery.NewClient(
		ctx,
		conf.ProjectId,
		option.WithHTTPClient(httpClient),
	)
	if err != nil {
		return nil, err
	}

	// Poor man ping
	di := client.Datasets(ctx)
	_, err = di.Next()
	if errors.Is(err, iterator.Done) {
		err = nil
	}
	if err != nil {
		return nil, exec.ConnectionError(err, IsAuthError)
	}

	return &BigQueryExecutor{client: client, conf: conf}, nil
}

// NewBigqueryExecutorFromClient wraps a client the caller has already built,
// without the connection check NewBigqueryExecutor makes.
func NewBigqueryExecutorFromClient(client *bigquery.Client, conf *BigQueryConf) *BigQueryExecutor {
	return &BigQueryExecutor{client: client, conf: conf}
}

// NewQuery builds a query job for sql that carries the query context of ctx:
// the SQL comment, which INFORMATION_SCHEMA.JOBS keeps in the query text, and
// the job labels. args become positional parameters. Every job we start is
// built here, so query-history readers can tell our own traffic apart.
func (e *BigQueryExecutor) NewQuery(ctx context.Context, sql string, args ...any) *bigquery.Query {
	query := e.client.Query(querycontext.AppendSQLComment(ctx, sql))
	if qc := querycontext.GetQueryContext(ctx); qc != nil {
		query.Labels = queryContextToBigQueryLabels(qc)
	}
	for _, arg := range args {
		query.Parameters = append(query.Parameters, bigquery.QueryParameter{Value: arg})
	}
	return query
}

func (e *BigQueryExecutor) Exec(ctx context.Context, sql string) error {
	query := e.NewQuery(ctx, sql)
	job, err := query.Run(ctx)
	if err != nil {
		return err
	}

	_, err = job.Read(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (e *BigQueryExecutor) QueryRowsIterator(
	ctx context.Context,
	sql string,
	args ...interface{},
) (*bigquery.RowIterator, error) {
	query := e.NewQuery(ctx, sql, args...)
	job, err := query.Run(ctx)
	if err != nil {
		return nil, err
	}

	it, err := job.Read(ctx)
	if err != nil {
		return nil, err
	}

	CollectBigQueryStats(ctx, job)
	return it, nil
}

// CollectBigQueryStats populates the DriverStats with BigQuery job statistics
// if a stats callback is registered in the context.
func CollectBigQueryStats(ctx context.Context, job *bigquery.Job) {
	ds := querystats.GetDriverStats(ctx)
	if ds == nil {
		return
	}
	status := job.LastStatus()
	if status == nil || status.Statistics == nil {
		return
	}

	stats := querystats.QueryStats{
		QueryID:   job.ID(),
		BytesRead: querystats.Int64Ptr(status.Statistics.TotalBytesProcessed),
	}

	if qs, ok := status.Statistics.Details.(*bigquery.QueryStatistics); ok {
		stats.BytesBilled = querystats.Int64Ptr(qs.TotalBytesBilled)
		stats.SlotMillis = querystats.Int64Ptr(qs.SlotMillis)
		stats.CacheHit = querystats.BoolPtr(qs.CacheHit)
	}

	ds.Set(stats)
}

// BigQuery label keys and values must contain only lowercase letters, numeric characters,
// underscores, and dashes. Keys must start with a letter. Max 63 chars for both key and value.
var (
	bigQueryLabelSanitizer = regexp.MustCompile(`[^a-z0-9_-]`)
	consecutiveUnderscores = regexp.MustCompile(`_{2,}`)
)

// queryContextToBigQueryLabels converts a query context to BigQuery-compatible job labels.
func queryContextToBigQueryLabels(qc querycontext.QueryContext) map[string]string {
	if len(qc) == 0 {
		return nil
	}
	labels := make(map[string]string, len(qc))
	for k, v := range qc {
		key := sanitizeBigQueryLabel(k)
		if len(key) == 0 {
			continue
		}
		// Keys must start with a letter
		if key[0] < 'a' || key[0] > 'z' {
			key = "l_" + key
		}
		if len(key) > 63 {
			key = key[:63]
		}
		val := sanitizeBigQueryLabel(v)
		if len(val) > 63 {
			val = val[:63]
		}
		labels[key] = val
	}
	return labels
}

func sanitizeBigQueryLabel(s string) string {
	s = strings.ToLower(s)
	s = bigQueryLabelSanitizer.ReplaceAllString(s, "_")
	s = consecutiveUnderscores.ReplaceAllString(s, "_")
	return s
}

func (e *BigQueryExecutor) GetBigQueryClient() *bigquery.Client {
	return e.client
}

func (e *BigQueryExecutor) Close() error {
	return e.client.Close()
}
