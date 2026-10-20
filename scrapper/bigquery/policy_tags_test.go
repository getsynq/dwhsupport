package bigquery

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	datacatalog "google.golang.org/api/datacatalog/v1"
	"google.golang.org/api/option"
)

const (
	piiTaxonomy     = "projects/governance/locations/eu/taxonomies/111"
	refusedTaxonomy = "projects/other/locations/us/taxonomies/222"
)

// fakeTaxonomy is a taxonomy of fakeDataCatalog: its display name and policy
// tags, or the status reading it fails with.
type fakeTaxonomy struct {
	displayName string
	policyTags  []fakePolicyTag
	status      int
}

type fakePolicyTag struct {
	id, displayName, parentId string
}

// fakeDataCatalog answers the Data Catalog REST calls the policy tag
// resolver makes, taxonomies.get and taxonomies.policyTags.list, and counts
// them. The list is served one policy tag per page.
type fakeDataCatalog struct {
	taxonomies map[string]fakeTaxonomy

	mu    sync.Mutex
	calls map[string]int
}

func (f *fakeDataCatalog) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	path := strings.TrimPrefix(r.URL.Path, "/v1/")
	f.mu.Lock()
	f.calls[path]++
	f.mu.Unlock()

	name, listing := strings.CutSuffix(path, "/policyTags")
	taxonomy, ok := f.taxonomies[name]
	switch {
	case !ok:
		fakeError(w, http.StatusNotFound, "taxonomy "+name+" not found")
	case taxonomy.status != 0:
		fakeError(w, taxonomy.status, "Permission 'datacatalog.taxonomies.get' denied on resource "+name)
	case !listing:
		_, _ = fmt.Fprintf(w, `{"name":%q,"displayName":%q}`, name, taxonomy.displayName)
	default:
		page := 0
		_, _ = fmt.Sscanf(r.URL.Query().Get("pageToken"), "page-%d", &page)
		if page >= len(taxonomy.policyTags) {
			_, _ = fmt.Fprint(w, `{}`)
			return
		}
		tag := taxonomy.policyTags[page]
		parent := ""
		if tag.parentId != "" {
			parent = name + "/policyTags/" + tag.parentId
		}
		next := ""
		if page+1 < len(taxonomy.policyTags) {
			next = fmt.Sprintf("page-%d", page+1)
		}
		_, _ = fmt.Fprintf(w, `{"policyTags":[{"name":%q,"displayName":%q,"parentPolicyTag":%q}],"nextPageToken":%q}`,
			name+"/policyTags/"+tag.id, tag.displayName, parent, next)
	}
}

func (f *fakeDataCatalog) callsTo(path string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[path]
}

func newFakeDataCatalog(t *testing.T, taxonomies map[string]fakeTaxonomy) (*fakeDataCatalog, func(context.Context) (*datacatalog.Service, error)) {
	t.Helper()
	fake := &fakeDataCatalog{taxonomies: taxonomies, calls: map[string]int{}}
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	return fake, func(ctx context.Context) (*datacatalog.Service, error) {
		return datacatalog.NewService(ctx,
			option.WithEndpoint(srv.URL),
			option.WithoutAuthentication(),
			option.WithHTTPClient(&http.Client{Timeout: 5 * time.Second}),
		)
	}
}

// governanceTaxonomies holds a taxonomy PII with the tree High > Email and
// Low, and a taxonomy the scan is refused.
func governanceTaxonomies() map[string]fakeTaxonomy {
	return map[string]fakeTaxonomy{
		piiTaxonomy: {displayName: "PII", policyTags: []fakePolicyTag{
			{id: "1", displayName: "High"},
			{id: "2", displayName: "Email", parentId: "1"},
			{id: "3", displayName: "Low"},
		}},
		refusedTaxonomy: {status: http.StatusForbidden},
	}
}

func policyTagField(name string, policyTags ...string) string {
	return fmt.Sprintf(`{"name":%q,"type":"STRING","policyTags":{"names":[%s]}}`, name,
		strings.Join(lo.Map(policyTags, func(tag string, _ int) string { return fmt.Sprintf("%q", tag) }), ","))
}

func fakeTableWithFields(id string, fields ...string) fakeTable {
	return fakeTable{id: id, json: `"type":"TABLE","schema":{"fields":[` + strings.Join(fields, ",") + `]}`}
}

type PolicyTagsSuite struct {
	suite.Suite
}

func TestPolicyTagsSuite(t *testing.T) {
	suite.Run(t, new(PolicyTagsSuite))
}

func (s *PolicyTagsSuite) catalog(dataCatalog func(context.Context) (*datacatalog.Service, error), tables ...fakeTable) map[string][]*scrapper.Tag {
	scr := newFakeMetadataScrapper(s.T(), fakeDataset{id: "crm", tables: tables})
	scr.newDataCatalog = dataCatalog
	rows, err := scr.QueryCatalog(context.Background())
	s.Require().NoError(err)
	return lo.SliceToMap(rows, func(r *scrapper.CatalogColumnRow) (string, []*scrapper.Tag) {
		return r.Table + "." + r.Column, r.ColumnTags
	})
}

func (s *PolicyTagsSuite) TestPolicyTagsBecomeTheTaxonomyAndThePathInsideIt() {
	_, dataCatalog := newFakeDataCatalog(s.T(), governanceTaxonomies())
	tags := s.catalog(dataCatalog, fakeTableWithFields("customers",
		`{"name":"id","type":"INTEGER"}`,
		policyTagField("email", piiTaxonomy+"/policyTags/2"),
		policyTagField("nickname", piiTaxonomy+"/policyTags/3"),
		policyTagField("high", piiTaxonomy+"/policyTags/1"),
		// A nested field's policy tags are not read, its column's own are.
		`{"name":"address","type":"RECORD","policyTags":{"names":["`+piiTaxonomy+`/policyTags/3"]},`+
			`"fields":[`+policyTagField("street", piiTaxonomy+"/policyTags/1")+`]}`,
	))

	s.Equal(map[string][]*scrapper.Tag{
		"customers.id":       nil,
		"customers.email":    {{TagName: "policytag.PII", TagValue: "High:Email"}},
		"customers.nickname": {{TagName: "policytag.PII", TagValue: "Low"}},
		"customers.high":     {{TagName: "policytag.PII", TagValue: "High"}},
		"customers.address":  {{TagName: "policytag.PII", TagValue: "Low"}},
	}, tags)
}

func (s *PolicyTagsSuite) TestAnUnreadablePolicyTagKeepsItsResourceName() {
	_, dataCatalog := newFakeDataCatalog(s.T(), governanceTaxonomies())
	tags := s.catalog(dataCatalog, fakeTableWithFields("customers",
		policyTagField("refused", refusedTaxonomy+"/policyTags/9"),
		policyTagField("deleted_taxonomy", "projects/governance/locations/eu/taxonomies/404/policyTags/1"),
		policyTagField("deleted_tag", piiTaxonomy+"/policyTags/99"),
		policyTagField("malformed", "not-a-policy-tag"),
	))

	s.Equal(map[string][]*scrapper.Tag{
		"customers.refused":          {{TagName: "policytag", TagValue: refusedTaxonomy + "/policyTags/9"}},
		"customers.deleted_taxonomy": {{TagName: "policytag", TagValue: "projects/governance/locations/eu/taxonomies/404/policyTags/1"}},
		"customers.deleted_tag":      {{TagName: "policytag", TagValue: piiTaxonomy + "/policyTags/99"}},
		"customers.malformed":        {{TagName: "policytag", TagValue: "not-a-policy-tag"}},
	}, tags)
}

func (s *PolicyTagsSuite) TestEachTaxonomyIsReadOncePerScan() {
	fake, dataCatalog := newFakeDataCatalog(s.T(), governanceTaxonomies())
	var tables []fakeTable
	for i := range 8 {
		tables = append(tables, fakeTableWithFields(fmt.Sprintf("t%d", i),
			policyTagField("email", piiTaxonomy+"/policyTags/2"),
			policyTagField("refused", refusedTaxonomy+"/policyTags/9"),
		))
	}
	tags := s.catalog(dataCatalog, tables...)

	s.Len(tags, 16)
	s.Equal(1, fake.callsTo(piiTaxonomy))
	s.Equal(3, fake.callsTo(piiTaxonomy+"/policyTags"), "one call per page")
	s.Equal(1, fake.callsTo(refusedTaxonomy))
	s.Zero(fake.callsTo(refusedTaxonomy + "/policyTags"))
}

func (s *PolicyTagsSuite) TestNoClientIsBuiltWithoutPolicyTags() {
	tags := s.catalog(func(context.Context) (*datacatalog.Service, error) {
		s.Fail("no column has a policy tag")
		return nil, errors.New("unexpected")
	}, fakeBaseTable("plain"))
	s.Equal(map[string][]*scrapper.Tag{"plain.id": nil}, tags)
}

func (s *PolicyTagsSuite) TestAClientThatCannotBeBuiltKeepsResourceNames() {
	tags := s.catalog(func(context.Context) (*datacatalog.Service, error) {
		return nil, errors.New("no credentials")
	}, fakeTableWithFields("customers", policyTagField("email", piiTaxonomy+"/policyTags/2")))
	s.Equal(map[string][]*scrapper.Tag{
		"customers.email": {{TagName: "policytag", TagValue: piiTaxonomy + "/policyTags/2"}},
	}, tags)
}

func TestPolicyTagPaths(t *testing.T) {
	tag := func(name, displayName, parent string) *datacatalog.GoogleCloudDatacatalogV1PolicyTag {
		return &datacatalog.GoogleCloudDatacatalogV1PolicyTag{Name: name, DisplayName: displayName, ParentPolicyTag: parent}
	}
	t.Run("a tree", func(t *testing.T) {
		require.Equal(t, map[string]string{"a": "A", "b": "A:B", "c": "A:B:C"}, policyTagPaths([]*datacatalog.GoogleCloudDatacatalogV1PolicyTag{
			tag("c", "C", "b"), tag("a", "A", ""), tag("b", "B", "a"),
		}))
	})
	t.Run("a parent not listed ends the path", func(t *testing.T) {
		require.Equal(t, map[string]string{"b": "B"}, policyTagPaths([]*datacatalog.GoogleCloudDatacatalogV1PolicyTag{tag("b", "B", "gone")}))
	})
	t.Run("a cycle is cut", func(t *testing.T) {
		paths := policyTagPaths([]*datacatalog.GoogleCloudDatacatalogV1PolicyTag{tag("a", "A", "b"), tag("b", "B", "a")})
		require.Len(t, strings.Split(paths["a"], ":"), maxPolicyTagDepth)
	})
	t.Run("nothing", func(t *testing.T) {
		require.Empty(t, policyTagPaths(nil))
	})
}
