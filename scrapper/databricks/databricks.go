package databricks

import (
	"context"
	"regexp"
	"strings"
	"time"

	"github.com/databricks/databricks-sdk-go"
	servicecatalog "github.com/databricks/databricks-sdk-go/service/catalog"
	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecdatabricks "github.com/getsynq/dwhsupport/exec/databricks"
	"github.com/getsynq/dwhsupport/lazy"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type DatabricksScrapperConf struct {
	dwhexecdatabricks.DatabricksConf
	// Scope limits every walk of Unity Catalog to the catalogs, schemas and tables it
	// accepts. It supersedes CatalogBlocklist, which can express nothing but
	// catalog-level exclusions.
	Scope *scope.ScopeFilter
	// CatalogBlocklist is a comma-separated list of catalog name patterns to exclude.
	//
	// Deprecated: use Scope, which covers schemas and tables as well. Still honoured
	// for callers that have not migrated, but only when Scope is unset — a Scope that
	// is present, even one carrying no rules, is what takes effect. This matches how
	// the cloud-side accounts.v1.Databricks.catalog_blocklist field it is fed from is
	// superseded by dwh_fetch_config.catalog.scope_filter.
	CatalogBlocklist           string
	FetchQueryLogs             bool
	RefreshTableMetrics        bool
	RefreshTableMetricsUseScan bool
	FetchTableTags             bool
	UseShowCreateTable         bool
	// QueryLogsStartTimeBuffer is the time buffer to add before the 'from' timestamp when fetching query logs.
	// This is needed because Databricks API filters by query start time, but we want queries that finished
	// in the target time range. A query that started before 'from' might finish after 'from'.
	// Default: 2 hours. Set to 0 to disable the buffer (use exact 'from' time).
	QueryLogsStartTimeBuffer *time.Duration
}

var _ scrapper.Scrapper = &DatabricksScrapper{}

type Executor interface {
	queryRows(ctx context.Context, q string, args ...interface{}) (*sqlx.Rows, error)
}

type DatabricksScrapper struct {
	client       *databricks.WorkspaceClient
	conf         *DatabricksScrapperConf
	lazyExecutor lazy.Lazy[*dwhexecdatabricks.DatabricksExecutor]
	scope        *scope.ScopeFilter
}

func (e *DatabricksScrapper) Capabilities() scrapper.Capabilities { return scrapper.Capabilities{} }

func (e *DatabricksScrapper) DialectType() string {
	return "databricks"
}

func (e *DatabricksScrapper) SqlDialect() sqldialect.Dialect {
	return sqldialect.NewDatabricksDialect()
}

func (e *DatabricksScrapper) Close() error {
	if e.lazyExecutor.Has() {
		client, _ := e.lazyExecutor.Get()
		if client != nil {
			return client.Close()
		}
	}
	return nil
}

func (e *DatabricksScrapper) ValidateConfiguration(ctx context.Context) ([]string, error) {
	//TODO implement me
	return nil, nil
}

var databricksReasonPhraseRegexp = regexp.MustCompile(`X-Databricks-Reason-Phrase: (.*)`)

func NewDatabricksScrapper(ctx context.Context, conf *DatabricksScrapperConf) (*DatabricksScrapper, error) {

	databricksConf := &databricks.Config{
		Host: conf.WorkspaceUrl,
	}
	conf.Auth.Configure(databricksConf)
	client, err := dwhexecdatabricks.NewWorkspaceClient(databricksConf)
	if err != nil {
		return nil, err
	}

	// Poor man ping
	_, err = client.CurrentUser.Me(ctx)
	if err != nil {
		// A workspace over its control-plane quota is not a workspace we cannot
		// authenticate against, and reporting it as one sends the customer to check
		// credentials that are fine.
		if dwhexecdatabricks.IsRateLimitError(err) {
			return nil, errors.Wrap(err, "failed to reach the workspace")
		}
		err := dwhexec.NewAuthError(err)
		errText := err.Error()
		ret := databricksReasonPhraseRegexp.FindAllStringSubmatch(errText, -1)
		for _, r := range ret {
			if len(r) == 2 {
				err = dwhexec.NewAuthError(errors.New(r[1]))
				break
			}
		}
		return nil, err
	}

	scopeFilter := ScopeFromConf(conf)

	executor := lazy.New(func() (*dwhexecdatabricks.DatabricksExecutor, error) {

		executor, err := dwhexecdatabricks.NewDatabricksExecutor(ctx, &conf.DatabricksConf)
		if err != nil {
			return nil, err
		}
		return executor, nil
	})

	return &DatabricksScrapper{client: client, conf: conf, scope: scopeFilter, lazyExecutor: executor}, nil
}

func (e *DatabricksScrapper) IsPermissionError(err error) bool {
	return dwhexecdatabricks.IsPermissionError(err)
}

func (e *DatabricksScrapper) GetApiClient() *databricks.WorkspaceClient {
	return e.client
}

// ThrottleStats reports how much the workspace's control-plane quota is shaping this
// process — how many requests it has refused, how long requests spent held back, and
// the spacing currently converged on. The throttle is shared by every client of the
// workspace in this process, so the numbers cover the lineage walk and any other
// enrolled producer too, not only this scrapper, and they run for the life of the
// process: a caller reporting on one scrape takes a snapshot either side of it and
// calls Since.
func (e *DatabricksScrapper) ThrottleStats() dwhexecdatabricks.ThrottleStats {
	return dwhexecdatabricks.ThrottleFor(e.conf.WorkspaceUrl).Stats()
}

// logThrottleStats says once, at the end of a scrape, that the workspace paced it —
// the difference between a scrape that wants a tighter scope and one that is broken.
// It reports what happened during this scrape rather than the throttle's running
// totals, and says nothing at all for a workspace that refused nothing, which is the
// normal case.
func (e *DatabricksScrapper) logThrottleStats(ctx context.Context, before dwhexecdatabricks.ThrottleStats) {
	stats := e.ThrottleStats().Since(before)
	if !stats.Paced() {
		return
	}
	logging.GetLogger(ctx).WithFields(logrus.Fields{
		"rate_limit_refusals": stats.Refusals,
		"throttle_wait_total": stats.Waited.Round(time.Second),
		"throttle_interval":   stats.Interval,
	}).Warn("Databricks rate limited this workspace; requests were paced to fit its quota")
}

func (e *DatabricksScrapper) Executor() (*dwhexecdatabricks.DatabricksExecutor, error) {
	return e.lazyExecutor.Get()
}

func (e *DatabricksScrapper) isIgnoredCatalog(catalogInfo servicecatalog.CatalogInfo) bool {
	if catalogInfo.CatalogType == servicecatalog.CatalogTypeSystemCatalog {
		return true
	}
	if catalogInfo.CatalogType == "INTERNAL_CATALOG" {
		return true
	}
	if catalogInfo.Name == "hive_metastore" {
		return true
	}
	if catalogInfo.Name == "personal" {
		return true
	}
	if strings.HasPrefix(catalogInfo.Name, "dev_") {
		return true
	}
	return false
}
