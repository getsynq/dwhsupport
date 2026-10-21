package athena

import (
	"database/sql/driver"
	"encoding/json"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
	dwhexecathena "github.com/getsynq/dwhsupport/exec/athena"
	"github.com/jmoiron/sqlx"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

// fakeGlueTable is one table of a fakeGlueEndpoint database, in the shape the Glue
// JSON API returns it (timestamps in epoch seconds).
type fakeGlueTable struct {
	Name       string            `json:"Name"`
	TableType  string            `json:"TableType,omitempty"`
	UpdateTime float64           `json:"UpdateTime,omitempty"`
	CreateTime float64           `json:"CreateTime,omitempty"`
	Parameters map[string]string `json:"Parameters,omitempty"`
}

// fakeGlueEndpoint answers the Glue JSON 1.1 calls QueryTableMetrics makes,
// GetDatabases and GetTables, from an in-memory catalog.
type fakeGlueEndpoint struct {
	databases map[string][]fakeGlueTable

	mu sync.Mutex
	// calls lists each call as "<operation> [<database>]".
	calls []string
}

func (f *fakeGlueEndpoint) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	target := strings.TrimPrefix(r.Header.Get("X-Amz-Target"), "AWSGlue.")
	body, _ := io.ReadAll(r.Body)
	var in struct {
		DatabaseName string `json:"DatabaseName"`
	}
	_ = json.Unmarshal(body, &in)

	f.mu.Lock()
	f.calls = append(f.calls, strings.TrimSpace(target+" "+in.DatabaseName))
	f.mu.Unlock()

	var out any
	switch target {
	case "GetDatabases":
		var list []map[string]string
		for name := range f.databases {
			list = append(list, map[string]string{"Name": name})
		}
		out = map[string]any{"DatabaseList": list}
	case "GetTables":
		out = map[string]any{"TableList": f.databases[in.DatabaseName]}
	default:
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"__type":"InvalidInputException","message":"unexpected call ` + target + `"}`))
		return
	}
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	_ = json.NewEncoder(w).Encode(out)
}

// newFakeGlueScrapper builds an AthenaScrapper whose Glue client talks to f
// and whose Athena statements go to db, which may be nil when the test runs
// none.
func newFakeGlueScrapper(t *testing.T, f *fakeGlueEndpoint, db *sqlx.DB) *AthenaScrapper {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)

	glueClient := awsglue.New(awsglue.Options{
		Region:       "eu-west-1",
		BaseEndpoint: aws.String(srv.URL),
		Credentials:  credentials.NewStaticCredentialsProvider("AKID", "SECRET", ""),
	})
	conf := &AthenaScrapperConf{AthenaConf: &dwhexecathena.AthenaConf{Region: "eu-west-1"}}
	return &AthenaScrapper{
		conf:     conf,
		executor: dwhexecathena.NewAthenaExecutorWithClients(conf.AthenaConf, "123456789012", glueClient, db),
	}
}

// icebergScans stands in for Athena in the Iceberg metadata scan: each table
// given answers with its row count, size and commit time, or fails when its
// values are nil, and every table a scan was sent for is recorded. sqlmock
// only consults the matcher while an expectation is open, so a test lists
// every Iceberg table, those it expects to stay unscanned too.
type icebergScans struct {
	mu      sync.Mutex
	scanned map[string]bool
}

var icebergScanTable = regexp.MustCompile(`FROM "[^"]*"\."([^"$]*)\$files"`)

func newIcebergScans(t *testing.T, rows map[string][]driver.Value) (*icebergScans, *sqlx.DB) {
	t.Helper()
	s := &icebergScans{scanned: map[string]bool{}}
	// sqlmock calls the matcher once per open expectation, so a scan is
	// recorded as a set member, not counted.
	matcher := sqlmock.QueryMatcherFunc(func(expected, actual string) error {
		m := icebergScanTable.FindStringSubmatch(actual)
		if m == nil {
			return errors.Errorf("not an iceberg scan: %s", actual)
		}
		s.mu.Lock()
		s.scanned[m[1]] = true
		s.mu.Unlock()
		if expected != m[1] {
			return errors.Errorf("scan of %s, expected %s", m[1], expected)
		}
		return nil
	})
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(matcher))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	mock.MatchExpectationsInOrder(false)
	for table, values := range rows {
		if values == nil {
			mock.ExpectQuery(table).WillReturnError(errors.Errorf("scan of %s refused", table))
			continue
		}
		mock.ExpectQuery(table).
			WillReturnRows(sqlmock.NewRows([]string{"row_count", "size_bytes", "last_committed_at"}).AddRow(values...))
	}
	return s, sqlx.NewDb(db, "sqlmock")
}

func (s *icebergScans) tables() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Sorted(maps.Keys(s.scanned))
}

func epoch(t time.Time) float64 { return float64(t.Unix()) }
