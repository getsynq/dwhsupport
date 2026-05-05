// Package athena provides a database/sql-style executor backed by
// github.com/uber/athenadriver. The driver speaks to Amazon Athena via the
// AWS Athena API (StartQueryExecution / GetQueryExecution / GetQueryResults)
// but exposes a database/sql-compatible surface, which lets us reuse the
// stdsql helpers other scrappers already use.
package athena

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awscredentials "github.com/aws/aws-sdk-go-v2/credentials"
	awsathena "github.com/aws/aws-sdk-go-v2/service/athena"
	awssts "github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/querycontext"
	"github.com/getsynq/dwhsupport/exec/querystats"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	"github.com/jmoiron/sqlx"
	athenadriver "github.com/influxdata/athenadriver/v2/go"
)

type AthenaConf struct {
	Region          string
	Workgroup       string // empty defaults to "primary"
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string // optional, only for short-lived STS creds
	Catalog         string // defaults to "AwsDataCatalog"
}

type Executor interface {
	stdsql.StdSqlExecutor
}

var _ stdsql.StdSqlExecutor = &AthenaExecutor{}

type AthenaExecutor struct {
	conf      *AthenaConf
	db        *sqlx.DB
	accountID string // sts:GetCallerIdentity, used as part of the canonical instance id
}

func (e *AthenaExecutor) GetDb() *sqlx.DB { return e.db }
func (e *AthenaExecutor) AccountID() string { return e.accountID }
func (e *AthenaExecutor) Region() string { return e.conf.Region }
func (e *AthenaExecutor) Workgroup() string {
	if e.conf.Workgroup != "" {
		return e.conf.Workgroup
	}
	return "primary"
}
func (e *AthenaExecutor) Catalog() string {
	if e.conf.Catalog != "" {
		return e.conf.Catalog
	}
	return "AwsDataCatalog"
}

// Instance returns the canonical instance identifier for an Athena endpoint:
// "<account-id>.<region>". This disambiguates two Athena integrations across
// AWS accounts that share a Glue database name.
func (e *AthenaExecutor) Instance() string {
	return fmt.Sprintf("%s.%s", e.accountID, e.conf.Region)
}

func NewAthenaExecutor(ctx context.Context, conf *AthenaConf) (*AthenaExecutor, error) {
	if conf.Region == "" {
		return nil, errors.New("athena: Region is required")
	}
	if conf.AccessKeyID == "" || conf.SecretAccessKey == "" {
		return nil, errors.New("athena: AccessKeyID and SecretAccessKey are required")
	}

	// Resolve the workgroup's configured S3 query result location via the
	// Athena API. The customer is expected to have set this on the workgroup
	// (with EnforceWorkGroupConfiguration=true so per-query overrides cannot
	// escape the data-scan cap). We only pass it to the driver because
	// athenadriver requires *some* output bucket in its DSN.
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(conf.Region),
		awsconfig.WithCredentialsProvider(awscredentials.NewStaticCredentialsProvider(
			conf.AccessKeyID, conf.SecretAccessKey, conf.SessionToken,
		)),
	)
	if err != nil {
		return nil, fmt.Errorf("athena: load aws config: %w", err)
	}

	stsClient := awssts.NewFromConfig(awsCfg)
	whoami, err := stsClient.GetCallerIdentity(ctx, &awssts.GetCallerIdentityInput{})
	if err != nil {
		return nil, exec.NewAuthError(fmt.Errorf("athena: sts:GetCallerIdentity: %w", err))
	}
	accountID := aws.ToString(whoami.Account)

	athenaClient := awsathena.NewFromConfig(awsCfg)
	wgName := conf.Workgroup
	if wgName == "" {
		wgName = "primary"
	}
	wg, err := athenaClient.GetWorkGroup(ctx, &awsathena.GetWorkGroupInput{WorkGroup: aws.String(wgName)})
	if err != nil {
		return nil, fmt.Errorf("athena: athena:GetWorkGroup %q: %w", wgName, err)
	}
	var outputLocation string
	if wg.WorkGroup != nil && wg.WorkGroup.Configuration != nil && wg.WorkGroup.Configuration.ResultConfiguration != nil {
		outputLocation = aws.ToString(wg.WorkGroup.Configuration.ResultConfiguration.OutputLocation)
	}
	if outputLocation == "" {
		return nil, fmt.Errorf("athena: workgroup %q has no query result location configured — set ResultConfiguration.OutputLocation on the workgroup", wgName)
	}

	driverConf, err := athenadriver.NewDefaultConfig(outputLocation, conf.Region, conf.AccessKeyID, conf.SecretAccessKey)
	if err != nil {
		return nil, fmt.Errorf("athena: build driver config: %w", err)
	}
	if conf.SessionToken != "" {
		driverConf.SetSessionToken(conf.SessionToken)
	}
	wgWrapper := athenadriver.NewDefaultWG(wgName, nil, nil)
	if err := driverConf.SetWorkGroup(wgWrapper); err != nil {
		return nil, fmt.Errorf("athena: set workgroup: %w", err)
	}
	driverConf.SetWGRemoteCreationAllowed(false)

	db, err := sqlx.Open(athenadriver.DriverName, driverConf.Stringify())
	if err != nil {
		return nil, fmt.Errorf("athena: open driver: %w", err)
	}
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, exec.NewAuthError(fmt.Errorf("athena: ping: %w", err))
	}

	return &AthenaExecutor{
		conf:      conf,
		db:        db,
		accountID: accountID,
	}, nil
}

func (e *AthenaExecutor) QueryRows(ctx context.Context, sql string, args ...interface{}) (*sqlx.Rows, error) {
	sql = querycontext.AppendSQLComment(ctx, trimRightSemicolons(sql))
	return e.db.QueryxContext(ctx, sql, args...)
}

func (e *AthenaExecutor) Select(ctx context.Context, dest any, query string, args ...any) error {
	query = querycontext.AppendSQLComment(ctx, trimRightSemicolons(query))
	collector, ctx := querystats.Start(ctx)
	defer collector.Finish()
	return e.db.SelectContext(ctx, dest, query, args...)
}

func (e *AthenaExecutor) Get(ctx context.Context, dest any, query string, args ...any) error {
	query = querycontext.AppendSQLComment(ctx, trimRightSemicolons(query))
	return e.db.GetContext(ctx, dest, query, args...)
}

func (e *AthenaExecutor) Exec(ctx context.Context, query string, args ...any) error {
	query = querycontext.AppendSQLComment(ctx, trimRightSemicolons(query))
	_, err := e.db.ExecContext(ctx, query, args...)
	return err
}

func (e *AthenaExecutor) Close() error {
	return e.db.Close()
}

func trimRightSemicolons(s string) string {
	for len(s) > 0 && (s[len(s)-1] == ';' || s[len(s)-1] == ' ' || s[len(s)-1] == '\n' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	return s
}
