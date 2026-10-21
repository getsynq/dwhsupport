package athena

import (
	"context"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/pkg/errors"
)

// glueTablesAPI is the part of the Glue client that lists databases and their
// tables, so a test can stand in for Glue.
type glueTablesAPI interface {
	GetDatabases(ctx context.Context, in *awsglue.GetDatabasesInput, opts ...func(*awsglue.Options)) (*awsglue.GetDatabasesOutput, error)
	GetTables(ctx context.Context, in *awsglue.GetTablesInput, opts ...func(*awsglue.Options)) (*awsglue.GetTablesOutput, error)
}

type glueTableKey struct {
	schema, table string
}

func newGlueTableKey(schema, table string) glueTableKey {
	return glueTableKey{schema: strings.ToLower(schema), table: strings.ToLower(table)}
}

// glueTableComments reads the comment of every table in scope from Glue, keyed
// by Glue database and table. Athena's information_schema has no table comment,
// so Glue is the only place that holds it.
//
// A comment is best-effort: when Glue cannot be read the tables keep no
// comment and the failure comes back as an OmittedDescriptions omission, of a
// Glue database when only that one failed, so the caller can return its rows
// with it. Only a done context is an error.
func (e *AthenaScrapper) glueTableComments(ctx context.Context) (map[glueTableKey]string, []scrapper.Omission, error) {
	glueClient := e.executor.GlueClient()
	if glueClient == nil {
		return nil, nil, nil
	}
	return readGlueTableComments(ctx, glueClient, e.executor.AccountID(), e.executor.Catalog(), e.effectiveScope(ctx))
}

func readGlueTableComments(
	ctx context.Context,
	glueClient glueTablesAPI,
	catalogID, athenaCatalog string,
	effScope *scope.ScopeFilter,
) (map[glueTableKey]string, []scrapper.Omission, error) {
	databases, err := listGlueDatabases(ctx, glueClient, catalogID)
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		logging.GetLogger(ctx).WithError(err).Warn("athena: reading table comments from Glue failed, tables are listed without them")
		return nil, []scrapper.Omission{{
			Database: athenaCatalog, Part: scrapper.OmittedDescriptions, Err: errors.Wrap(err, "glue:GetDatabases"),
		}}, nil
	}

	comments := map[glueTableKey]string{}
	var omissions []scrapper.Omission
	for _, dbName := range databases {
		if !effScope.IsSchemaAccepted(athenaCatalog, dbName) {
			continue
		}
		err := forEachGlueTable(ctx, glueClient, catalogID, dbName, func(t gluetypes.Table) {
			name := aws.ToString(t.Name)
			if !effScope.IsObjectAccepted(athenaCatalog, dbName, name) {
				return
			}
			if comment := glueTableComment(t); comment != "" {
				comments[newGlueTableKey(dbName, name)] = comment
			}
		})
		if err != nil {
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}
			logging.GetLogger(ctx).WithField("glue_database", dbName).WithError(err).
				Warn("athena: reading table comments from Glue failed, the database's tables are listed without them")
			omissions = append(omissions, scrapper.Omission{
				Database: athenaCatalog, Schema: dbName, Part: scrapper.OmittedDescriptions,
				Err: errors.Wrapf(err, "glue:GetTables %q", dbName),
			})
		}
	}
	return comments, omissions, nil
}

func forEachGlueTable(ctx context.Context, glueClient glueTablesAPI, catalogID, dbName string, fn func(gluetypes.Table)) error {
	var nextToken *string
	for {
		resp, err := glueClient.GetTables(ctx, &awsglue.GetTablesInput{
			CatalogId:    aws.String(catalogID),
			DatabaseName: aws.String(dbName),
			NextToken:    nextToken,
		})
		if err != nil {
			return err
		}
		for _, t := range resp.TableList {
			fn(t)
		}
		nextToken = resp.NextToken
		if nextToken == nil {
			return nil
		}
	}
}

// glueTableComment is the table's Glue description, which the Glue API and
// console set, or else its `comment` parameter, where a Hive DDL's COMMENT and
// `TBLPROPERTIES ('comment'=…)` keep it. A view has only the description:
// Athena sets the `comment` parameter of every view it creates to "Presto View".
func glueTableComment(t gluetypes.Table) string {
	if description := aws.ToString(t.Description); description != "" || isGlueView(t) {
		return description
	}
	return t.Parameters["comment"]
}

// withOmittedComments returns the rows with the error a query ends with: the
// omissions when the caller accepts an incomplete result, nothing otherwise, so
// a caller that never asked keeps reading the tables as it always did.
func withOmittedComments[T any](ctx context.Context, rows []T, omissions []scrapper.Omission) ([]T, error) {
	if !scrapper.IncompleteResultsAccepted(ctx) {
		return rows, nil
	}
	return rows, scrapper.Incomplete(omissions)
}
