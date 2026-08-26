package scrapper_test

import (
	"context"
	"database/sql/driver"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/suite"
)

type ScanAllSuite struct {
	suite.Suite
}

func TestScanAllSuite(t *testing.T) {
	suite.Run(t, &ScanAllSuite{})
}

// resultSet opens a real *sqlx.Rows over a canned result set, so a scan runs
// through the same driver machinery it does against a warehouse. No warehouse is
// needed to reproduce any of this: the shape of the result set is the whole bug.
func (s *ScanAllSuite) resultSet(columns []string, values ...[]driver.Value) *sqlx.Rows {
	db, mock, err := sqlmock.New()
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = db.Close() })

	canned := sqlmock.NewRows(columns)
	for _, value := range values {
		canned = canned.AddRow(value...)
	}
	mock.ExpectQuery("SELECT").WillReturnRows(canned)

	rows, err := sqlx.NewDb(db, "sqlmock").Queryx("SELECT 1")
	s.Require().NoError(err)
	s.T().Cleanup(func() { _ = rows.Close() })
	return rows
}

// An account with QUOTED_IDENTIFIERS_IGNORE_CASE = TRUE stores every quoted
// identifier upper-cased, so the lower-case aliases every catalog query in this
// package asks for come back as DATABASE, SCHEMA, TABLE. The setting is a
// migration-compatibility switch on the customer's own account and nothing in
// the product can turn it off, so a scan that only matches the case it asked for
// loses the entire catalog of an account that has it on.
func (s *ScanAllSuite) TestTablesScanWhenTheAccountUppercasesQuotedAliases() {
	rows := s.resultSet(
		[]string{"DATABASE", "SCHEMA", "TABLE", "TABLE_TYPE", "DESCRIPTION", "IS_VIEW", "IS_TABLE"},
		[]driver.Value{"ANALYTICS", "PUBLIC", "ORDERS", "BASE TABLE", "", false, true},
	)

	got, err := scrapper.ScanAll[scrapper.TableRow](
		context.Background(), rows, "ANALYTICS.information_schema.tables",
	)

	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.Equal("ANALYTICS", got[0].Database)
	s.Equal("PUBLIC", got[0].Schema)
	s.Equal("ORDERS", got[0].Table)
	s.True(got[0].IsTable)
}

func (s *ScanAllSuite) TestSqlDefinitionsScanWhenTheAccountUppercasesQuotedAliases() {
	rows := s.resultSet(
		[]string{"DATABASE", "SCHEMA", "TABLE", "TABLE_TYPE", "SQL", "IS_VIEW", "IS_TABLE", "IS_MATERIALIZED_VIEW"},
		[]driver.Value{"ANALYTICS", "PUBLIC", "ORDERS_V", "VIEW", "select 1", true, false, false},
	)

	got, err := scrapper.ScanAll[scrapper.SqlDefinitionRow](
		context.Background(), rows, "ANALYTICS.information_schema.views",
	)

	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.Equal("ORDERS_V", got[0].Table)
	s.Equal("select 1", got[0].Sql)
	s.True(got[0].IsView)
}

func (s *ScanAllSuite) TestCatalogColumnsScanWhenTheAccountUppercasesQuotedAliases() {
	rows := s.resultSet(
		[]string{"DATABASE", "SCHEMA", "TABLE", "COLUMN", "TYPE", "POSITION", "TABLE_TYPE", "IS_VIEW", "IS_TABLE"},
		[]driver.Value{"ANALYTICS", "PUBLIC", "ORDERS", "ID", "NUMBER", int64(1), "BASE TABLE", false, true},
	)

	got, err := scrapper.ScanAll[scrapper.CatalogColumnRow](
		context.Background(), rows, "ANALYTICS.information_schema.columns",
	)

	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.Equal("ID", got[0].Column)
	s.EqualValues(1, got[0].Position)
}

func (s *ScanAllSuite) TestTableMetricsScanWhenTheAccountUppercasesQuotedAliases() {
	updatedAt := time.Date(2026, 8, 26, 13, 29, 0, 0, time.UTC)
	rows := s.resultSet(
		[]string{"DATABASE", "SCHEMA", "TABLE", "ROW_COUNT", "UPDATED_AT", "SIZE_BYTES"},
		[]driver.Value{"ANALYTICS", "PUBLIC", "ORDERS", int64(42), updatedAt, int64(4096)},
	)

	got, err := scrapper.ScanAll[scrapper.TableMetricsRow](
		context.Background(), rows, "ANALYTICS.information_schema.tables",
	)

	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.Require().NotNil(got[0].RowCount)
	s.EqualValues(42, *got[0].RowCount)
	s.Require().NotNil(got[0].UpdatedAt)
	s.Equal(updatedAt, got[0].UpdatedAt.UTC())
}

func (s *ScanAllSuite) TestSchemasScanWhenTheAccountUppercasesQuotedAliases() {
	rows := s.resultSet(
		[]string{"DATABASE", "SCHEMA", "DESCRIPTION", "SCHEMA_TYPE", "SCHEMA_OWNER"},
		[]driver.Value{"ANALYTICS", "PUBLIC", nil, nil, "SYNQ_ROLE"},
	)

	got, err := scrapper.ScanAll[scrapper.SchemaRow](
		context.Background(), rows, "ANALYTICS.information_schema.schemata",
	)

	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.Equal("PUBLIC", got[0].Schema)
	s.Require().NotNil(got[0].SchemaOwner)
	s.Equal("SYNQ_ROLE", *got[0].SchemaOwner)
}

// The ordinary account, where the aliases come back exactly as they were asked
// for. Matching either case has to mean matching this one too.
func (s *ScanAllSuite) TestTablesScanWhenTheAccountKeepsQuotedAliasesAsWritten() {
	rows := s.resultSet(
		[]string{"database", "schema", "table", "table_type", "description", "is_view", "is_table"},
		[]driver.Value{"analytics", "public", "orders", "BASE TABLE", "an order", false, true},
	)

	got, err := scrapper.ScanAll[scrapper.TableRow](
		context.Background(), rows, "analytics.information_schema.tables",
	)

	s.Require().NoError(err)
	s.Require().Len(got, 1)
	s.Equal("orders", got[0].Table)
	s.Require().NotNil(got[0].Description)
	s.Equal("an order", *got[0].Description)
}
