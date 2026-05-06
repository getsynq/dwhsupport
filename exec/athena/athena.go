// Package athena provides a database/sql-style executor backed by
// github.com/influxdata/athenadriver/v2. The driver speaks to Amazon Athena
// via the AWS Athena API (StartQueryExecution / GetQueryExecution /
// GetQueryResults) but exposes a database/sql-compatible surface, which lets
// us reuse the stdsql helpers other scrappers already use.
package athena

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	awscredentials "github.com/aws/aws-sdk-go-v2/credentials"
	awsstscreds "github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	awsathena "github.com/aws/aws-sdk-go-v2/service/athena"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
	awssts "github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/getsynq/dwhsupport/exec"
	"github.com/getsynq/dwhsupport/exec/querycontext"
	"github.com/getsynq/dwhsupport/exec/querystats"
	"github.com/getsynq/dwhsupport/exec/stdsql"
	athenadriver "github.com/influxdata/athenadriver/v2/go"
	"github.com/jmoiron/sqlx"
)

// AthenaConf carries everything the executor needs to open a connection.
//
// Authentication is resolved in priority order:
//  1. Explicit AccessKeyID + SecretAccessKey (+ optional SessionToken).
//  2. AwsProfile — named profile from ~/.aws/credentials.
//  3. AWS default credential chain (env vars, shared config, EC2/ECS/EKS instance
//     role) — ONLY when AllowDefaultChain is set. Refused otherwise to prevent
//     a customer configuration with empty fields from accidentally inheriting
//     the host process's identity (a real risk on the SYNQ cloud backend).
//
// If RoleArn is set on top of any of the above, the executor wraps the base
// credentials in an STS AssumeRole provider.
type AthenaConf struct {
	Region    string
	Workgroup string // empty defaults to "primary"
	Catalog   string // empty defaults to "AwsDataCatalog"

	// Static credentials. Highest priority.
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string

	// Named AWS shared-config profile. Used only when static credentials are absent.
	AwsProfile string

	// AssumeRole. Wraps whichever base credentials resolved above.
	RoleArn         string
	ExternalID      string
	RoleSessionName string // empty defaults to "synq-athena"

	// AllowDefaultChain opts into the AWS default credential chain (env vars,
	// shared config, EC2/ECS/EKS instance role) when no explicit auth method
	// is configured. Safe to enable on the on-prem agent host — that's the
	// customer's own AWS environment. MUST stay false on the SYNQ cloud
	// backend so a misconfigured customer integration cannot fall through
	// to SYNQ's own AWS identity.
	AllowDefaultChain bool
}

type Executor interface {
	stdsql.StdSqlExecutor
}

var _ stdsql.StdSqlExecutor = &AthenaExecutor{}

type AthenaExecutor struct {
	conf         *AthenaConf
	awsCfg       aws.Config
	athenaClient *awsathena.Client
	glueClient   *awsglue.Client
	db           *sqlx.DB
	accountID    string // sts:GetCallerIdentity, used as part of the canonical instance id
}

func (e *AthenaExecutor) GetDb() *sqlx.DB                 { return e.db }
func (e *AthenaExecutor) AccountID() string               { return e.accountID }
func (e *AthenaExecutor) Region() string                  { return e.conf.Region }
func (e *AthenaExecutor) AthenaClient() *awsathena.Client { return e.athenaClient }
func (e *AthenaExecutor) GlueClient() *awsglue.Client     { return e.glueClient }
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
// "<account-id>.<region>". Disambiguates two Athena integrations across AWS
// accounts that share a Glue database name.
func (e *AthenaExecutor) Instance() string {
	return fmt.Sprintf("%s.%s", e.accountID, e.conf.Region)
}

func NewAthenaExecutor(ctx context.Context, conf *AthenaConf) (*AthenaExecutor, error) {
	if conf.Region == "" {
		return nil, errors.New("athena: Region is required")
	}

	awsCfg, err := buildAwsConfig(ctx, conf)
	if err != nil {
		return nil, err
	}

	// sts:GetCallerIdentity validates auth + gives us the account ID for the instance string.
	stsClient := awssts.NewFromConfig(awsCfg)
	whoami, err := stsClient.GetCallerIdentity(ctx, &awssts.GetCallerIdentityInput{})
	if err != nil {
		return nil, exec.NewAuthError(fmt.Errorf("athena: sts:GetCallerIdentity: %w", err))
	}
	accountID := aws.ToString(whoami.Account)

	athenaClient := awsathena.NewFromConfig(awsCfg)
	glueClient := awsglue.NewFromConfig(awsCfg)

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
		return nil, fmt.Errorf(
			"athena: workgroup %q has no query result location configured — set ResultConfiguration.OutputLocation on the workgroup",
			wgName,
		)
	}

	// Resolve credentials once and pass the materialized triple to the driver.
	// The influxdata driver doesn't accept a credentials provider, so for
	// AssumeRole / profile / chain paths we snapshot the active creds here.
	// For long-lived processes using STS creds the executor must be recreated
	// before they expire (default 1h, max 12h for AssumeRole).
	creds, err := awsCfg.Credentials.Retrieve(ctx)
	if err != nil {
		return nil, exec.NewAuthError(fmt.Errorf("athena: resolve credentials: %w", err))
	}

	driverConf, err := athenadriver.NewDefaultConfig(outputLocation, conf.Region, creds.AccessKeyID, creds.SecretAccessKey)
	if err != nil {
		return nil, fmt.Errorf("athena: build driver config: %w", err)
	}
	if creds.SessionToken != "" {
		driverConf.SetSessionToken(creds.SessionToken)
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
		conf:         conf,
		awsCfg:       awsCfg,
		athenaClient: athenaClient,
		glueClient:   glueClient,
		db:           db,
		accountID:    accountID,
	}, nil
}

// buildAwsConfig assembles an aws.Config from the configured authentication
// method. Resolution order: static keys → profile → default chain. RoleArn
// wraps whichever base credentials resolved.
func buildAwsConfig(ctx context.Context, conf *AthenaConf) (aws.Config, error) {
	loadOpts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(conf.Region),
	}
	hasExplicitAuth := false
	switch {
	case conf.AccessKeyID != "" && conf.SecretAccessKey != "":
		loadOpts = append(loadOpts, awsconfig.WithCredentialsProvider(
			awscredentials.NewStaticCredentialsProvider(conf.AccessKeyID, conf.SecretAccessKey, conf.SessionToken),
		))
		hasExplicitAuth = true
	case conf.AccessKeyID != "" || conf.SecretAccessKey != "":
		return aws.Config{}, errors.New("athena: AccessKeyID and SecretAccessKey must be provided together")
	case conf.AwsProfile != "":
		loadOpts = append(loadOpts, awsconfig.WithSharedConfigProfile(conf.AwsProfile))
		hasExplicitAuth = true
	}
	if conf.RoleArn != "" {
		hasExplicitAuth = true
	}
	if !hasExplicitAuth && !conf.AllowDefaultChain {
		// Refuse to fall through to the AWS default credential chain (env vars,
		// EC2/ECS/EKS instance role). On the cloud backend this would attach
		// to SYNQ's own identity if a customer config had empty fields.
		// On-prem agent paths must explicitly opt in via AllowDefaultChain.
		return aws.Config{}, errors.New(
			"athena: no authentication method configured — set AccessKeyID+SecretAccessKey, AwsProfile, RoleArn, or AllowDefaultChain",
		)
	}
	cfg, err := awsconfig.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return aws.Config{}, fmt.Errorf("athena: load aws config: %w", err)
	}

	if conf.RoleArn != "" {
		stsClient := awssts.NewFromConfig(cfg)
		sessionName := conf.RoleSessionName
		if sessionName == "" {
			sessionName = "synq-athena"
		}
		provider := awsstscreds.NewAssumeRoleProvider(stsClient, conf.RoleArn, func(o *awsstscreds.AssumeRoleOptions) {
			o.RoleSessionName = sessionName
			if conf.ExternalID != "" {
				o.ExternalID = aws.String(conf.ExternalID)
			}
		})
		cfg.Credentials = aws.NewCredentialsCache(provider)
	}

	return cfg, nil
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
