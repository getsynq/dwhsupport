package athena

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
	dwhexecathena "github.com/getsynq/dwhsupport/exec/athena"
	"github.com/getsynq/dwhsupport/scrapper/scope"
)

// fakeGlueTable is one table of a fakeGlue database, in the shape the Glue
// JSON API returns it (timestamps in epoch seconds).
type fakeGlueTable struct {
	Name       string            `json:"Name"`
	TableType  string            `json:"TableType,omitempty"`
	UpdateTime float64           `json:"UpdateTime,omitempty"`
	CreateTime float64           `json:"CreateTime,omitempty"`
	Parameters map[string]string `json:"Parameters,omitempty"`
}

// fakeGlue answers the Glue JSON 1.1 calls QueryTableMetrics makes,
// GetDatabases and GetTables, from an in-memory catalog.
type fakeGlue struct {
	databases map[string][]fakeGlueTable

	mu sync.Mutex
	// calls lists each call as "<operation> [<database>]".
	calls []string
}

func (f *fakeGlue) ServeHTTP(w http.ResponseWriter, r *http.Request) {
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

// newFakeGlueScrapper builds an AthenaScrapper whose Glue client talks to f.
func newFakeGlueScrapper(t *testing.T, f *fakeGlue, scopeFilter *scope.ScopeFilter) *AthenaScrapper {
	t.Helper()
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)

	glueClient := awsglue.New(awsglue.Options{
		Region:       "eu-west-1",
		BaseEndpoint: aws.String(srv.URL),
		Credentials:  credentials.NewStaticCredentialsProvider("AKID", "SECRET", ""),
	})
	conf := &AthenaScrapperConf{
		AthenaConf: &dwhexecathena.AthenaConf{Region: "eu-west-1"},
		Scope:      scopeFilter,
	}
	return &AthenaScrapper{
		conf:     conf,
		executor: dwhexecathena.NewAthenaExecutorWithGlue(conf.AthenaConf, "123456789012", glueClient),
	}
}

func epoch(t time.Time) float64 { return float64(t.Unix()) }
