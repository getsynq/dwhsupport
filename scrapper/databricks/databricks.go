package databricks

import (
	"context"
	"regexp"
	"strings"

	"github.com/databricks/databricks-sdk-go"
	servicecatalog "github.com/databricks/databricks-sdk-go/service/catalog"
	"github.com/databricks/databricks-sdk-go/useragent"
	"github.com/getsynq/dwhsupport/blocklist"
	dwhexec "github.com/getsynq/dwhsupport/exec"
	dwhexecdatabricks "github.com/getsynq/dwhsupport/exec/databricks"
	"github.com/getsynq/dwhsupport/lazy"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/sqldialect"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
)

type DatabricksScrapperConf struct {
	dwhexecdatabricks.DatabricksConf
	CatalogBlocklist           string
	FetchQueryLogs             bool
	RefreshTableMetrics        bool
	RefreshTableMetricsUseScan bool
	FetchTableTags             bool
	UseShowCreateTable         bool
}

var _ scrapper.Scrapper = &DatabricksScrapper{}

type Executor interface {
	queryRows(ctx context.Context, q string, args ...interface{}) (*sqlx.Rows, error)
}

type DatabricksScrapper struct {
	client       *databricks.WorkspaceClient
	conf         *DatabricksScrapperConf
	lazyExecutor lazy.Lazy[*dwhexecdatabricks.DatabricksExecutor]
	blocklist    blocklist.Blocklist
}

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

	useragent.WithProduct("synq", "1.0.0")

	databricksConf := &databricks.Config{
		Host: conf.WorkspaceUrl,
	}
	conf.Auth.Configure(databricksConf)
	client, err := databricks.NewWorkspaceClient(databricksConf)
	if err != nil {
		return nil, err
	}

	// Poor man ping
	_, err = client.CurrentUser.Me(ctx)
	if err != nil {
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

	blocklist := blocklist.NewBlocklistFromString(conf.CatalogBlocklist)

	executor := lazy.New(func() (*dwhexecdatabricks.DatabricksExecutor, error) {

		executor, err := dwhexecdatabricks.NewDatabricksExecutor(ctx, &conf.DatabricksConf)
		if err != nil {
			return nil, err
		}
		return executor, nil
	})

	return &DatabricksScrapper{client: client, conf: conf, blocklist: blocklist, lazyExecutor: executor}, nil
}

func (e *DatabricksScrapper) IsPermissionError(err error) bool {
	if err == nil {
		return false
	}
	errMsg := err.Error()
	return strings.Contains(errMsg, "PERMISSION_DENIED") ||
		strings.Contains(errMsg, "ACCESS_DENIED") ||
		strings.Contains(errMsg, "does not have permission")
}

func (e *DatabricksScrapper) GetApiClient() *databricks.WorkspaceClient {
	return e.client
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
