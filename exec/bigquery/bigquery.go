package bigquery

import (
	"context"
	"regexp"
	"strings"

	"github.com/pkg/errors"
	"golang.org/x/oauth2"

	"cloud.google.com/go/bigquery"
	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/querycontext"
	"github.com/getsynq/dwhsupport/exec/querystats"
	_ "github.com/lib/pq"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// BigQueryConf configures the BigQuery executor connection.
// Authentication precedence: AccessToken > CredentialsJson > CredentialsFile.
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
}

type Executor interface {
	QueryRowsIterator(ctx context.Context, q string, args ...interface{}) (*bigquery.RowIterator, error)
}

type BigQueryExecutor struct {
	conf   *BigQueryConf
	client *bigquery.Client
}

func NewBigqueryExecutor(ctx context.Context, conf *BigQueryConf) (*BigQueryExecutor, error) {

	var options []option.ClientOption
	options = append(options, option.WithUserAgent("synq-bq-client-v1.0.0"))
	if conf.AccessToken != "" {
		tokenSource := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: conf.AccessToken})
		options = append(options, option.WithTokenSource(tokenSource))
	} else if len(conf.CredentialsJson) > 0 {
		options = append(options, option.WithCredentialsJSON([]byte(conf.CredentialsJson)))
	} else if len(conf.CredentialsFile) > 0 {
		options = append(options, option.WithCredentialsFile(conf.CredentialsFile))
	}

	client, err := bigquery.NewClient(
		ctx,
		conf.ProjectId,
		options...,
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
		return nil, exec.NewAuthError(err)
	}

	return &BigQueryExecutor{client: client, conf: conf}, nil
}

func (e *BigQueryExecutor) Exec(ctx context.Context, sql string) error {
	sql = querycontext.AppendSQLComment(ctx, sql)
	query := e.client.Query(sql)
	if qc := querycontext.GetQueryContext(ctx); qc != nil {
		query.Labels = queryContextToBigQueryLabels(qc)
	}
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
	sql = querycontext.AppendSQLComment(ctx, sql)
	query := e.client.Query(sql)
	if qc := querycontext.GetQueryContext(ctx); qc != nil {
		query.Labels = queryContextToBigQueryLabels(qc)
	}
	for _, arg := range args {
		query.Parameters = append(query.Parameters, bigquery.QueryParameter{Value: arg})
	}

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
var bigQueryLabelSanitizer = regexp.MustCompile(`[^a-z0-9_-]`)
var consecutiveUnderscores = regexp.MustCompile(`_{2,}`)

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
