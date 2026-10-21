package athena

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	pkgerrors "github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeGlue serves GetDatabases and GetTables from pages of canned tables, one
// page per call, and fails the databases named in failTables.
type fakeGlue struct {
	databases     []string
	databasesErr  error
	tables        map[string][][]gluetypes.Table
	failTables    map[string]error
	tablesQueried []string
}

func (f *fakeGlue) GetDatabases(
	_ context.Context,
	_ *awsglue.GetDatabasesInput,
	_ ...func(*awsglue.Options),
) (*awsglue.GetDatabasesOutput, error) {
	if f.databasesErr != nil {
		return nil, f.databasesErr
	}
	out := &awsglue.GetDatabasesOutput{}
	for _, name := range f.databases {
		out.DatabaseList = append(out.DatabaseList, gluetypes.Database{Name: aws.String(name)})
	}
	return out, nil
}

func (f *fakeGlue) GetTables(
	_ context.Context,
	in *awsglue.GetTablesInput,
	_ ...func(*awsglue.Options),
) (*awsglue.GetTablesOutput, error) {
	db := aws.ToString(in.DatabaseName)
	f.tablesQueried = append(f.tablesQueried, db)
	if err := f.failTables[db]; err != nil {
		return nil, err
	}
	page := 0
	if in.NextToken != nil {
		page = int(aws.ToString(in.NextToken)[0] - '0')
	}
	pages := f.tables[db]
	if page >= len(pages) {
		return &awsglue.GetTablesOutput{}, nil
	}
	out := &awsglue.GetTablesOutput{TableList: pages[page]}
	if page+1 < len(pages) {
		out.NextToken = aws.String(string(rune('0' + page + 1)))
	}
	return out, nil
}

func glueTable(name string, description *string, params map[string]string) gluetypes.Table {
	return gluetypes.Table{Name: aws.String(name), Description: description, Parameters: params}
}

// athenaView is a view the way Athena writes it to Glue.
func athenaView(name string, description *string) gluetypes.Table {
	t := glueTable(name, description, map[string]string{"comment": "Presto View", "presto_view": "true"})
	t.TableType = aws.String("VIRTUAL_VIEW")
	return t
}

func TestReadGlueTableComments(t *testing.T) {
	glue := &fakeGlue{
		databases: []string{"sales", "Logs"},
		tables: map[string][][]gluetypes.Table{
			"sales": {
				{
					glueTable("orders", aws.String("Orders placed"), nil),
					glueTable("hive_table", nil, map[string]string{"comment": "From Hive DDL"}),
				},
				{
					glueTable("both", aws.String("Glue description"), map[string]string{"comment": "Hive comment"}),
					glueTable("empty_description", aws.String(""), map[string]string{"comment": "Hive comment"}),
					glueTable("no_comment", nil, map[string]string{"numRows": "10"}),
				},
			},
			"Logs": {{
				glueTable("Events", aws.String("Raw events"), nil),
				athenaView("events_view", nil),
				athenaView("described_view", aws.String("Described in Glue")),
			}},
		},
	}

	comments, omissions, err := readGlueTableComments(context.Background(), glue, "123", "AwsDataCatalog", nil)
	require.NoError(t, err)
	assert.Empty(t, omissions)
	assert.Equal(t, map[glueTableKey]string{
		newGlueTableKey("sales", "orders"):            "Orders placed",
		newGlueTableKey("sales", "hive_table"):        "From Hive DDL",
		newGlueTableKey("sales", "both"):              "Glue description",
		newGlueTableKey("sales", "empty_description"): "Hive comment",
		newGlueTableKey("logs", "events"):             "Raw events",
		newGlueTableKey("logs", "described_view"):     "Described in Glue",
	}, comments, "every page is read, a table without a comment and a view's \"Presto View\" are left out, keys ignore case")
}

func TestReadGlueTableCommentsHonoursScope(t *testing.T) {
	glue := &fakeGlue{
		databases: []string{"sales", "hr"},
		tables: map[string][][]gluetypes.Table{
			"sales": {{glueTable("orders", aws.String("Orders"), nil), glueTable("refunds", aws.String("Refunds"), nil)}},
			"hr":    {{glueTable("salaries", aws.String("Salaries"), nil)}},
		},
	}
	sc := &scope.ScopeFilter{Include: []scope.ScopeRule{{Schema: "sales", Table: "orders"}}}

	comments, omissions, err := readGlueTableComments(context.Background(), glue, "123", "AwsDataCatalog", sc)
	require.NoError(t, err)
	assert.Empty(t, omissions)
	assert.Equal(t, map[glueTableKey]string{newGlueTableKey("sales", "orders"): "Orders"}, comments)
	assert.Equal(t, []string{"sales"}, glue.tablesQueried, "a database out of scope is not listed")
}

func TestReadGlueTableCommentsOmitsADatabaseGlueRefuses(t *testing.T) {
	refused := pkgerrors.New("AccessDeniedException: not authorized to perform glue:GetTables")
	glue := &fakeGlue{
		databases:  []string{"sales", "hr"},
		tables:     map[string][][]gluetypes.Table{"sales": {{glueTable("orders", aws.String("Orders"), nil)}}},
		failTables: map[string]error{"hr": refused},
	}

	comments, omissions, err := readGlueTableComments(context.Background(), glue, "123", "AwsDataCatalog", nil)
	require.NoError(t, err)
	assert.Equal(t, map[glueTableKey]string{newGlueTableKey("sales", "orders"): "Orders"}, comments)
	require.Len(t, omissions, 1)
	assert.Equal(t, "AwsDataCatalog", omissions[0].Database)
	assert.Equal(t, "hr", omissions[0].Schema)
	assert.Equal(t, scrapper.OmittedDescriptions, omissions[0].Part)
	assert.ErrorIs(t, omissions[0].Err, refused)
}

func TestReadGlueTableCommentsOmitsTheCatalogWhenDatabasesFail(t *testing.T) {
	refused := pkgerrors.New("AccessDeniedException: not authorized to perform glue:GetDatabases")
	glue := &fakeGlue{databasesErr: refused}

	comments, omissions, err := readGlueTableComments(context.Background(), glue, "123", "AwsDataCatalog", nil)
	require.NoError(t, err)
	assert.Empty(t, comments)
	require.Len(t, omissions, 1)
	assert.Equal(t, scrapper.Omission{Database: "AwsDataCatalog", Part: scrapper.OmittedDescriptions, Err: omissions[0].Err}, omissions[0])
	assert.ErrorIs(t, omissions[0].Err, refused)
}

func TestReadGlueTableCommentsFailsOnADoneContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	glue := &fakeGlue{databasesErr: context.Canceled}
	_, _, err := readGlueTableComments(ctx, glue, "123", "AwsDataCatalog", nil)
	assert.ErrorIs(t, err, context.Canceled)

	glue = &fakeGlue{databases: []string{"sales"}, failTables: map[string]error{"sales": context.Canceled}}
	_, _, err = readGlueTableComments(ctx, glue, "123", "AwsDataCatalog", nil)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestWithOmittedCommentsOnlyForACallerThatAccepts(t *testing.T) {
	rows := []*scrapper.TableRow{{Table: "orders"}}
	omissions := []scrapper.Omission{{Database: "AwsDataCatalog", Schema: "hr", Part: scrapper.OmittedDescriptions, Err: pkgerrors.New("refused")}}

	got, err := withOmittedComments(context.Background(), rows, omissions)
	assert.NoError(t, err, "a caller that never asked gets no incomplete result")
	assert.Equal(t, rows, got)

	got, err = withOmittedComments(scrapper.WithIncompleteResults(context.Background()), rows, omissions)
	incomplete, ok := scrapper.AsIncomplete(err)
	require.True(t, ok, "got %v", err)
	assert.Equal(t, omissions, incomplete.Omissions)
	assert.Equal(t, rows, got, "the rows come with the omissions")

	_, err = withOmittedComments(scrapper.WithIncompleteResults(context.Background()), rows, nil)
	assert.NoError(t, err)
}
