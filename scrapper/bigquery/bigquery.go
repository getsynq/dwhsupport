package bigquery

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	dwhexecbigquery "github.com/getsynq/dwhsupport/exec/bigquery"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/api/cloudresourcemanager/v1"

	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cloud.google.com/go/bigquery"
	"github.com/getsynq/dwhsupport/scrapper"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// BigQueryScrapperConf configures BigQuery metadata scrapping.
type BigQueryScrapperConf struct {
	dwhexecbigquery.BigQueryConf
	// Blocklist is a comma-separated list of dataset name patterns to exclude.
	Blocklist string
	// RateLimitCfg overrides the default rate limit configuration for BQ API calls.
	RateLimitCfg *RateLimitConfig
	// Datasets is an explicit list of dataset names to scrape. When set, only these
	// datasets are queried instead of listing all datasets in the project via API.
	// This is required for customers who grant permissions at the dataset level and
	// lack project-level bigquery.datasets.list permission.
	Datasets []string
}

type Executor interface {
	queryRows(ctx context.Context, q string, args ...interface{}) (*bigquery.RowIterator, error)
}

var _ scrapper.Scrapper = &BigQueryScrapper{}

type BigQueryScrapper struct {
	conf         *BigQueryScrapperConf
	scope        *scope.ScopeFilter
	executor     *dwhexecbigquery.BigQueryExecutor
	rateLimitCfg RateLimitConfig
}

func (e *BigQueryScrapper) IsPermissionError(err error) bool {
	return dwhexecbigquery.IsPermissionError(err)
}

func (e *BigQueryScrapper) Capabilities() scrapper.Capabilities {
	return scrapper.Capabilities{
		ConstraintsViaQueryTables: true,
	}
}

func (e *BigQueryScrapper) DialectType() string {
	return "bigquery"
}

func (e *BigQueryScrapper) SqlDialect() sqldialect.Dialect {
	return sqldialect.NewBigQueryDialect()
}

var BaseExpectedPermissions = []string{
	"bigquery.datasets.get",
	"bigquery.datasets.getIamPolicy",
	"bigquery.jobs.create",
	"bigquery.jobs.get",
	"bigquery.jobs.list",
	"bigquery.jobs.listAll",
	"bigquery.routines.get",
	"bigquery.routines.list",
	"bigquery.tables.get",
	"bigquery.tables.list",
	"resourcemanager.projects.get",
}

func NewBigQueryScrapper(ctx context.Context, conf *BigQueryScrapperConf) (*BigQueryScrapper, error) {
	executor, err := dwhexecbigquery.NewBigqueryExecutor(ctx, &conf.BigQueryConf)
	if err != nil {
		return nil, err
	}

	scopeFilter := ScopeFromConf(conf)

	rateLimitCfg := DefaultRateLimitConfig
	if conf.RateLimitCfg != nil {
		rateLimitCfg = *conf.RateLimitCfg
	}

	return &BigQueryScrapper{
		executor:     executor,
		conf:         conf,
		scope:        scopeFilter,
		rateLimitCfg: rateLimitCfg,
	}, nil
}

func (e *BigQueryScrapper) Executor() *dwhexecbigquery.BigQueryExecutor {
	return e.executor
}

// listDatasets returns the datasets to scrape. When conf.Datasets is set,
// returns those explicitly; otherwise lists all visible datasets via the API.
func (e *BigQueryScrapper) listDatasets(ctx context.Context) ([]*bigquery.Dataset, error) {
	if len(e.conf.Datasets) > 0 {
		client := e.executor.GetBigQueryClient()
		seen := make(map[string]bool)
		var result []*bigquery.Dataset
		for _, name := range e.conf.Datasets {
			if seen[name] {
				continue
			}
			seen[name] = true
			result = append(result, client.Dataset(name))
		}
		return result, nil
	}

	client := e.executor.GetBigQueryClient()

	// A non-Done error is terminal — the iterator does not advance past it, so
	// `continue` would busy-loop forever. Retry handles genuine rate limits.
	result, err := withRateLimitRetry(ctx, e.rateLimitCfg, func(ctx context.Context) ([]*bigquery.Dataset, error) {
		it := client.Datasets(ctx)
		it.ListHidden = true

		var result []*bigquery.Dataset
		for {
			ds, err := it.Next()
			if errors.Is(err, iterator.Done) {
				break
			}
			if err != nil {
				return nil, err
			}
			result = append(result, ds)
		}
		return result, nil
	})
	if err != nil {
		return nil, e.scrapeError(ctx, "datasets.list", "", "", err)
	}
	return result, nil
}

// tablesListProgressInterval throttles progress events so an enormous dataset
// can't attach thousands of events to the span; a final summary is always
// emitted regardless. It is a var so tests can tighten it.
var tablesListProgressInterval = 5 * time.Second

// listTableIDs lists every table ID in a dataset. The listing runs through
// withRateLimitRetry so a genuine rate-limit backs off, and each attempt is
// bounded by CallTimeout so a stalled page can't hang the scrape.
//
// A non-Done error from the iterator is TERMINAL: the BigQuery iterator does not
// advance past it, so retrying the same iterator would get the same error and
// busy-loop forever — the root cause of the multi-hour catalog hangs. The
// listing (in collectTableIDs) always returns on error, letting the caller fail
// the scrape (failing is safe: a failed fetch publishes nothing, so no assets
// are wrongly marked deleted), and emits progress/done span events so a slow or
// stalled listing is observable.
func (e *BigQueryScrapper) listTableIDs(ctx context.Context, datasetID string) ([]string, error) {
	ids, err := withRateLimitRetry(ctx, e.rateLimitCfg, func(ctx context.Context) ([]string, error) {
		it := e.executor.GetBigQueryClient().Dataset(datasetID).Tables(ctx)
		return collectTableIDs(ctx, datasetID, it)
	})
	if err != nil {
		// 404 means the dataset itself is gone — typically a linked/shared
		// dataset whose source was deleted (its Metadata() still resolves but
		// listing its tables 404s). It genuinely no longer exists, so skip it and
		// let the rest of the scrape proceed. Access-denied and other errors
		// still fail the scrape: we must not silently drop — and thereby delete —
		// tables we simply can't see right now.
		if errIsNotFound(err) {
			logging.GetLogger(ctx).
				WithField("executor", "bigquery").
				WithField("bq_dataset", datasetID).
				WithError(err).
				Warn("skipping dataset that no longer exists (tables.list returned not-found)")
			return nil, nil
		}
		return nil, e.scrapeError(ctx, "tables.list", datasetID, "", err)
	}
	return ids, nil
}

// collectTableIDs iterates a dataset's table listing, returning every table ID.
// It emits a throttled progress event as the count grows plus a final done event
// on the active span, so we can see how far a slow listing got before its
// deadline: a hung dataset produces no progress (and no done — the deadline
// fires first), while a merely huge one shows the count climbing right up to the
// CallTimeout. The BigQuery client's own per-request spans are dropped at the
// export filter, so these events are how we observe listing progress.
//
// A non-Done error is returned immediately: the BigQuery iterator never advances
// past it, so re-reading would busy-loop — the root cause of the multi-hour
// catalog hangs.
func collectTableIDs(ctx context.Context, datasetID string, it *bigquery.TableIterator) ([]string, error) {
	span := trace.SpanFromContext(ctx)

	var ids []string
	start := time.Now()
	lastEvent := start
	for {
		table, err := it.Next()
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}
		ids = append(ids, table.TableID)
		if time.Since(lastEvent) >= tablesListProgressInterval {
			span.AddEvent("bigquery.tables.list.progress", trace.WithAttributes(
				attribute.String("bq.dataset", datasetID),
				attribute.Int("bq.tables_list.tables", len(ids)),
				attribute.Int64("bq.tables_list.elapsed_ms", time.Since(start).Milliseconds()),
			))
			lastEvent = time.Now()
		}
	}
	span.AddEvent("bigquery.tables.list.done", trace.WithAttributes(
		attribute.String("bq.dataset", datasetID),
		attribute.Int("bq.tables_list.tables", len(ids)),
		attribute.Int64("bq.tables_list.elapsed_ms", time.Since(start).Milliseconds()),
	))
	return ids, nil
}

func (e *BigQueryScrapper) queryRows(ctx context.Context, q string, args ...interface{}) (*bigquery.RowIterator, error) {
	query := e.executor.GetBigQueryClient().Query(q)
	job, err := query.Run(ctx)
	if err != nil {
		return nil, err
	}

	it, err := job.Read(ctx)
	if err != nil {
		return nil, err
	}

	return it, nil
}

func (e *BigQueryScrapper) ValidateConfiguration(ctx context.Context) ([]string, error) {
	// When explicit datasets are configured, permissions are granted at dataset level
	// and project-level TestIamPermissions would report false negatives.
	if len(e.conf.Datasets) > 0 {
		logging.GetLogger(ctx).Info("skipping project-level permission check: explicit datasets configured")
		return nil, nil
	}

	crm, err := cloudresourcemanager.NewService(
		ctx,
		option.WithCredentialsJSON([]byte(e.conf.CredentialsJson)),
		option.WithUserAgent("synq-bq-client-v1.0.0"),
	)
	if err != nil {
		return nil, err
	}

	var warnings []string

	if permissions, err := crm.Projects.TestIamPermissions(e.conf.ProjectId, &cloudresourcemanager.TestIamPermissionsRequest{
		Permissions: BaseExpectedPermissions,
	}).Context(ctx).Do(); err == nil {
		gotPermissions := permissions.Permissions
		sort.Strings(gotPermissions)

		missingPermissions, _ := lo.Difference(BaseExpectedPermissions, gotPermissions)
		if len(missingPermissions) > 0 {
			logging.GetLogger(ctx).WithField("missing_permissions", missingPermissions).Info("missing BigQuery permissions")
			warnings = append(
				warnings,
				fmt.Sprintf(
					"Some permissions are missing, this might cause not all features to work properly: %s",
					strings.Join(missingPermissions, ", "),
				),
			)
		}
	} else if info, ok := serviceDisabledFromErr(err); ok {
		// A required Google API is disabled in the customer's project. This is not
		// fatal for scraping — the IAM pre-flight check simply can't run — so we
		// surface an actionable warning rather than a raw 403 or a hard failure.
		service := info.service
		if service == "" {
			service = "a required Google"
		}
		msg := fmt.Sprintf(
			"Could not verify BigQuery permissions because the %s API is not enabled in project %s.",
			service, e.conf.ProjectId,
		)
		if info.activationURL != "" {
			msg += fmt.Sprintf(" Enable it at %s and retry.", info.activationURL)
		}
		logging.GetLogger(ctx).
			WithError(err).
			WithField("service", info.service).
			Warn("skipping BigQuery permission check: required Google API disabled")
		warnings = append(warnings, msg)
	} else {
		logging.GetLogger(ctx).WithError(err).Error("failed to test BigQuery permissions")
	}
	return warnings, nil
}

// serviceDisabledInfo carries the actionable bits of a Google "API disabled"
// error: which service is disabled and where to enable it.
type serviceDisabledInfo struct {
	service       string
	activationURL string
}

// serviceDisabledFromErr detects a Google "SERVICE_DISABLED" error and extracts
// the disabled service and its activation URL. Google returns a 403 whose
// Details carry a google.rpc.ErrorInfo with reason "SERVICE_DISABLED" and
// metadata naming the service and activation URL — e.g. when the Cloud Resource
// Manager API is not enabled in the customer's project (QUA-113). Returns
// ok=false for nil, non-Google errors, and ordinary permission denials.
func serviceDisabledFromErr(err error) (serviceDisabledInfo, bool) {
	var gerr *googleapi.Error
	if !errors.As(err, &gerr) {
		return serviceDisabledInfo{}, false
	}
	for _, d := range gerr.Details {
		m, ok := d.(map[string]interface{})
		if !ok {
			continue
		}
		if reason, _ := m["reason"].(string); reason != "SERVICE_DISABLED" {
			continue
		}
		info := serviceDisabledInfo{}
		if md, ok := m["metadata"].(map[string]interface{}); ok {
			info.service, _ = md["service"].(string)
			info.activationURL, _ = md["activationUrl"].(string)
		}
		return info, true
	}
	return serviceDisabledInfo{}, false
}

func (e *BigQueryScrapper) Close() error {
	return e.executor.Close()
}

func isPrivateDataset(id string) bool {
	if strings.HasPrefix(id, "_script") || strings.HasPrefix(id, "_project_level_cache") || (strings.HasPrefix(id, "_") && len(id) == 41) {
		return true
	}
	return false
}

func errIsNotFound(err error) bool {
	if err == nil {
		return false
	}
	if e := status.Code(err); e == codes.NotFound {
		return true
	}
	var e *googleapi.Error
	if errors.As(err, &e) {
		if e.Code == 404 {
			return true
		}
	}
	return false
}

func errIsAccessDenied(err error) bool {
	if err == nil {
		return false
	}
	if e := status.Code(err); e == codes.PermissionDenied {
		return true
	}
	var e *googleapi.Error
	if errors.As(err, &e) {
		if e.Code == 403 {
			return true
		}
	}
	return false
}

func errIsRateLimited(err error) bool {
	if err == nil {
		return false
	}
	if e := status.Code(err); e == codes.ResourceExhausted {
		return true
	}
	var e *googleapi.Error
	if errors.As(err, &e) {
		if e.Code == 429 {
			return true
		}
	}
	return false
}
