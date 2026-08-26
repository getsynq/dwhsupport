package scrapper

import (
	"context"
	"database/sql/driver"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type RowScannerSuite struct {
	suite.Suite
}

func TestRowScannerSuite(t *testing.T) {
	suite.Run(t, new(RowScannerSuite))
}

type scanTarget struct {
	ID        string     `db:"ID"`
	Name      string     `db:"NAME"`
	Count     int64      `db:"COUNT"`
	Optional  *string    `db:"OPTIONAL"`
	At        time.Time  `db:"AT"`
	AtOrNever *time.Time `db:"AT_OR_NEVER"`
	// Filled in by the caller, never by the warehouse.
	Injected string
	Ignored  string `db:"-"`
}

func mockRows(t *testing.T, columns []string, values ...[]driver.Value) *sqlx.Rows {
	t.Helper()

	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	mockRows := sqlmock.NewRows(columns)
	for _, row := range values {
		mockRows.AddRow(row...)
	}
	mock.ExpectQuery("SELECT").WillReturnRows(mockRows)

	rows, err := sqlx.NewDb(db, "sqlmock").QueryxContext(context.Background(), "SELECT *")
	require.NoError(t, err)
	t.Cleanup(func() { _ = rows.Close() })

	return rows
}

func (s *RowScannerSuite) scanAll(rows *sqlx.Rows, scanner *RowScanner[scanTarget]) []scanTarget {
	var result []scanTarget
	for rows.Next() {
		var row scanTarget
		s.Require().NoError(scanner.Scan(rows, &row))
		result = append(result, row)
	}
	s.Require().NoError(rows.Err())
	return result
}

func (s *RowScannerSuite) TestScansExactMatch() {
	at := time.Date(2026, 8, 10, 9, 0, 0, 0, time.UTC)
	rows := mockRows(
		s.T(),
		[]string{"ID", "NAME", "COUNT", "OPTIONAL", "AT", "AT_OR_NEVER"},
		[]driver.Value{"a", "first", int64(1), "set", at, at},
		[]driver.Value{"b", "second", int64(2), nil, at, nil},
	)

	scanner, err := NewRowScanner[scanTarget](rows)
	s.Require().NoError(err)
	s.Empty(scanner.UnknownColumns())
	s.Empty(scanner.MissingColumns())

	result := s.scanAll(rows, scanner)
	s.Require().Len(result, 2)
	s.Equal("a", result[0].ID)
	s.Equal("first", result[0].Name)
	s.Equal(int64(1), result[0].Count)
	s.Require().NotNil(result[0].Optional)
	s.Equal("set", *result[0].Optional)
	s.Equal(at, result[0].At.UTC())
	s.Require().NotNil(result[0].AtOrNever)
	s.Nil(result[1].Optional)
	s.Nil(result[1].AtOrNever)
}

// A column the struct has no field for is the vendor-adds-a-column case, and it
// must cost nothing but a log line.
func (s *RowScannerSuite) TestDiscardsUnknownColumns() {
	at := time.Date(2026, 8, 10, 9, 0, 0, 0, time.UTC)
	rows := mockRows(
		s.T(),
		[]string{"ID", "BRAND_NEW", "NAME", "COUNT", "ANOTHER_NEW", "OPTIONAL", "AT", "AT_OR_NEVER"},
		[]driver.Value{"a", "vendor value", "first", int64(1), int64(7), nil, at, nil},
	)

	scanner, err := NewRowScanner[scanTarget](rows)
	s.Require().NoError(err)
	s.Equal([]string{"BRAND_NEW", "ANOTHER_NEW"}, scanner.UnknownColumns())
	s.Empty(scanner.MissingColumns())
	scanner.LogColumnDrift(context.Background(), "TEST.SOURCE")

	result := s.scanAll(rows, scanner)
	s.Require().Len(result, 1)
	s.Equal("a", result[0].ID)
	s.Equal("first", result[0].Name)
	s.Equal(int64(1), result[0].Count)
}

// The other direction: a column that went away leaves its field zeroed and the
// rest of the row intact.
func (s *RowScannerSuite) TestReportsMissingColumns() {
	rows := mockRows(
		s.T(),
		[]string{"ID", "NAME"},
		[]driver.Value{"a", "first"},
	)

	scanner, err := NewRowScanner[scanTarget](rows)
	s.Require().NoError(err)
	s.Empty(scanner.UnknownColumns())
	s.Equal([]string{"COUNT", "OPTIONAL", "AT", "AT_OR_NEVER"}, scanner.MissingColumns())

	result := s.scanAll(rows, scanner)
	s.Require().Len(result, 1)
	s.Equal("a", result[0].ID)
	s.Zero(result[0].Count)
	s.Nil(result[0].Optional)
	s.True(result[0].At.IsZero())
}

func (s *RowScannerSuite) TestRequireColumns() {
	rows := mockRows(s.T(), []string{"ID", "NAME"}, []driver.Value{"a", "first"})

	scanner, err := NewRowScanner[scanTarget](rows)
	s.Require().NoError(err)

	s.NoError(scanner.RequireColumns("ID", "NAME"))
	// Case-insensitive, because a warehouse decides the case it reports.
	s.NoError(scanner.RequireColumns("id"))

	err = scanner.RequireColumns("ID", "COUNT", "AT")
	s.Require().Error(err)
	s.Contains(err.Error(), "COUNT")
	s.Contains(err.Error(), "AT")
	s.NotContains(err.Error(), "ID,")
}

// Snowflake reports uppercase column names, other platforms lowercase; the tag is
// written once and must match either.
func (s *RowScannerSuite) TestMatchesColumnsCaseInsensitively() {
	rows := mockRows(s.T(), []string{"id", "Name"}, []driver.Value{"a", "first"})

	scanner, err := NewRowScanner[scanTarget](rows)
	s.Require().NoError(err)
	s.Empty(scanner.UnknownColumns())

	result := s.scanAll(rows, scanner)
	s.Require().Len(result, 1)
	s.Equal("a", result[0].ID)
	s.Equal("first", result[0].Name)
}

// Untagged fields belong to the scrapper, and `db:"-"` opts out explicitly.
// Neither may be claimed by a column that happens to share the name.
func (s *RowScannerSuite) TestIgnoresFieldsWithoutColumnTag() {
	rows := mockRows(s.T(), []string{"ID", "INJECTED", "IGNORED"}, []driver.Value{"a", "from db", "from db"})

	scanner, err := NewRowScanner[scanTarget](rows)
	s.Require().NoError(err)
	s.Equal([]string{"INJECTED", "IGNORED"}, scanner.UnknownColumns())

	result := s.scanAll(rows, scanner)
	s.Require().Len(result, 1)
	s.Empty(result[0].Injected)
	s.Empty(result[0].Ignored)
}

type embeddedColumns struct {
	Extra string `db:"EXTRA"`
}

type embeddingTarget struct {
	embeddedColumns
	ID string `db:"ID"`
}

func (s *RowScannerSuite) TestScansEmbeddedStructFields() {
	rows := mockRows(s.T(), []string{"EXTRA", "ID"}, []driver.Value{"e", "a"})

	scanner, err := NewRowScanner[embeddingTarget](rows)
	s.Require().NoError(err)
	s.Empty(scanner.UnknownColumns())
	s.Empty(scanner.MissingColumns())

	s.Require().True(rows.Next())
	var row embeddingTarget
	s.Require().NoError(scanner.Scan(rows, &row))
	s.Equal("e", row.Extra)
	s.Equal("a", row.ID)
}

type duplicateTagTarget struct {
	First  string `db:"ID"`
	Second string `db:"id"`
}

func (s *RowScannerSuite) TestRejectsDuplicateColumnTag() {
	rows := mockRows(s.T(), []string{"ID"}, []driver.Value{"a"})

	_, err := NewRowScanner[duplicateTagTarget](rows)
	s.Require().Error(err)
	s.Contains(err.Error(), "claimed by more than one field")
}

func (s *RowScannerSuite) TestRejectsNonStructTarget() {
	rows := mockRows(s.T(), []string{"ID"}, []driver.Value{"a"})

	_, err := NewRowScanner[string](rows)
	s.Require().Error(err)
	s.Contains(err.Error(), "must be a struct")
}

// A result set repeating a column must not have the later copy overwrite the
// earlier one.
func (s *RowScannerSuite) TestDiscardsRepeatedColumn() {
	rows := mockRows(s.T(), []string{"ID", "NAME", "ID"}, []driver.Value{"first", "n", "second"})

	scanner, err := NewRowScanner[scanTarget](rows)
	s.Require().NoError(err)
	s.Equal([]string{"ID"}, scanner.UnknownColumns())

	result := s.scanAll(rows, scanner)
	s.Require().Len(result, 1)
	s.Equal("first", result[0].ID)
}
