package bigquery

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/getsynq/dwhsupport/blocklist"
	dwhexecbigquery "github.com/getsynq/dwhsupport/exec/bigquery"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"google.golang.org/api/cloudresourcemanager/v1"

	"google.golang.org/api/googleapi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"cloud.google.com/go/bigquery"
	"github.com/getsynq/dwhsupport/scrapper"
	"google.golang.org/api/option"
)

type BigQueryScrapperConf struct {
	dwhexecbigquery.BigQueryConf
	Blocklist         string
	FetchQueryLogs    bool
	FetchTableMetrics bool
}

type Executor interface {
	queryRows(ctx context.Context, q string, args ...interface{}) (*bigquery.RowIterator, error)
}

var _ scrapper.Scrapper = &BigQueryScrapper{}

type BigQueryScrapper struct {
	conf      *BigQueryScrapperConf
	blocklist blocklist.Blocklist
	executor  *dwhexecbigquery.BigQueryExecutor
}

func (e *BigQueryScrapper) Dialect() string {
	return "bigquery"
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
	"bigquery.tables.getData",
	"bigquery.tables.list",
	"resourcemanager.projects.get",
	//"storage.buckets.get",
	//"storage.buckets.list",
	//"storage.objects.get",
	//"storage.objects.list",
}

func NewBigQueryScrapper(ctx context.Context, conf *BigQueryScrapperConf) (*BigQueryScrapper, error) {

	executor, err := dwhexecbigquery.NewBigqueryExecutor(ctx, &conf.BigQueryConf)
	if err != nil {
		return nil, err
	}

	blocklist := blocklist.NewBlocklistFromString(conf.Blocklist)

	return &BigQueryScrapper{executor: executor, conf: conf, blocklist: blocklist}, nil
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
	crm, err := cloudresourcemanager.NewService(ctx, option.WithCredentialsJSON([]byte(e.conf.CredentialsJson)), option.WithUserAgent("synq-bq-client-v1.0.0"))
	if err != nil {
		return nil, err
	}

	var warnings []string

	expectedPermissions := BaseExpectedPermissions

	if permissions, err := crm.Projects.TestIamPermissions(e.conf.ProjectId, &cloudresourcemanager.TestIamPermissionsRequest{
		Permissions: expectedPermissions,
	}).Context(ctx).Do(); err == nil {
		gotPermissions := permissions.Permissions
		sort.Strings(gotPermissions)

		missingPermissions, _ := lo.Difference(expectedPermissions, gotPermissions)
		if len(missingPermissions) > 0 {
			logging.GetLogger(ctx).WithField("missing_permissions", missingPermissions).Info("missing BigQuery permissions")
			warnings = append(warnings, fmt.Sprintf("Some permissions are missing, this might cause not all features to work properly: %s", strings.Join(missingPermissions, ", ")))
		}
	} else {
		logging.GetLogger(ctx).WithError(err).Error("failed to test BigQuery permissions")
	}
	return warnings, nil
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
