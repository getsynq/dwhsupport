package bigquery

import (
	"context"
	"strings"
	"sync"

	dwhexecbigquery "github.com/getsynq/dwhsupport/exec/bigquery"
	"github.com/getsynq/dwhsupport/logging"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/pkg/errors"
	datacatalog "google.golang.org/api/datacatalog/v1"
	"google.golang.org/api/option"
)

const (
	// maxPolicyTagDepth bounds the walk up a policy tag's parents. Data Catalog
	// nests them five deep at most, so a longer walk is a broken tree.
	maxPolicyTagDepth = 16
)

// dataCatalogServiceFor builds the Data Catalog client a scan reads policy
// taxonomies with, acting as the same identity as the BigQuery client, or nil
// when the configuration did not opt in to FetchPolicyTags.
func dataCatalogServiceFor(conf *BigQueryScrapperConf) func(ctx context.Context) (*datacatalog.Service, error) {
	if !conf.FetchPolicyTags {
		return nil
	}
	return func(ctx context.Context) (*datacatalog.Service, error) {
		httpClient, err := dwhexecbigquery.NewHTTPClient(ctx, &conf.BigQueryConf, datacatalog.CloudPlatformScope)
		if err != nil {
			return nil, err
		}
		return datacatalog.NewService(ctx, option.WithHTTPClient(httpClient))
	}
}

// policyTagResolver turns the policy tag resource names BigQuery puts on a
// column into tags naming the taxonomy and the path to the policy tag inside
// it, by their display names. Each taxonomy is read once per scan, by whichever
// worker asks first, and a taxonomy that cannot be read is logged once. With no
// client (FetchPolicyTags off) nothing is read and every policy tag keeps its
// resource name.
type policyTagResolver struct {
	newService   func(ctx context.Context) (*datacatalog.Service, error)
	rateLimitCfg RateLimitConfig

	serviceOnce sync.Once
	service     *datacatalog.Service
	serviceErr  error

	mu         sync.Mutex
	taxonomies map[string]*policyTaxonomy
}

// policyTaxonomy is one taxonomy as a scan read it.
type policyTaxonomy struct {
	once        sync.Once
	displayName string
	// paths holds each policy tag's display path below the taxonomy, its
	// ancestors first, joined with ':'.
	paths map[string]string
	err   error
}

func newPolicyTagResolver(newService func(ctx context.Context) (*datacatalog.Service, error), rateLimitCfg RateLimitConfig) *policyTagResolver {
	return &policyTagResolver{newService: newService, rateLimitCfg: rateLimitCfg, taxonomies: map[string]*policyTaxonomy{}}
}

// tags resolves the policy tags of one column. A policy tag whose taxonomy
// cannot be read, or that its taxonomy no longer lists, is named by its
// taxonomy's resource name and keeps its own resource name as the value.
func (r *policyTagResolver) tags(ctx context.Context, names []string) []*scrapper.Tag {
	var tags []*scrapper.Tag
	for _, name := range names {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		tags = append(tags, r.tag(ctx, name))
	}
	return tags
}

func (r *policyTagResolver) tag(ctx context.Context, name string) *scrapper.Tag {
	taxonomyName, _, found := strings.Cut(name, "/policyTags/")
	unresolved := &scrapper.Tag{TagName: taxonomyName, TagValue: name, Kind: scrapper.TagKindPolicyTag}
	if !found || r.newService == nil {
		return unresolved
	}
	taxonomy := r.taxonomy(ctx, taxonomyName)
	if taxonomy.err != nil {
		return unresolved
	}
	path, ok := taxonomy.paths[name]
	if !ok {
		return unresolved
	}
	return &scrapper.Tag{TagName: taxonomy.displayName, TagValue: path, Kind: scrapper.TagKindPolicyTag}
}

func (r *policyTagResolver) taxonomy(ctx context.Context, name string) *policyTaxonomy {
	r.mu.Lock()
	taxonomy, ok := r.taxonomies[name]
	if !ok {
		taxonomy = &policyTaxonomy{}
		r.taxonomies[name] = taxonomy
	}
	r.mu.Unlock()

	taxonomy.once.Do(func() {
		taxonomy.displayName, taxonomy.paths, taxonomy.err = r.readTaxonomy(ctx, name)
		if taxonomy.err != nil {
			logging.GetLogger(ctx).WithError(taxonomy.err).WithField("taxonomy", name).
				Warn("cannot read policy tag taxonomy, its policy tags keep their resource names")
		}
	})
	return taxonomy
}

func (r *policyTagResolver) readTaxonomy(ctx context.Context, name string) (string, map[string]string, error) {
	r.serviceOnce.Do(func() {
		r.service, r.serviceErr = r.newService(ctx)
	})
	if r.serviceErr != nil {
		return "", nil, errors.Wrap(r.serviceErr, "failed to create the Data Catalog client")
	}

	taxonomy, err := withRateLimitRetry(ctx, r.rateLimitCfg, func(ctx context.Context) (*datacatalog.GoogleCloudDatacatalogV1Taxonomy, error) {
		return r.service.Projects.Locations.Taxonomies.Get(name).Context(ctx).Do()
	})
	if err != nil {
		return "", nil, errors.Wrapf(err, "failed to get taxonomy %s", name)
	}

	var policyTags []*datacatalog.GoogleCloudDatacatalogV1PolicyTag
	_, err = withRateLimitRetry(ctx, r.rateLimitCfg, func(ctx context.Context) (struct{}, error) {
		policyTags = nil
		return struct{}{}, r.service.Projects.Locations.Taxonomies.PolicyTags.List(name).Context(ctx).Pages(
			ctx, func(page *datacatalog.GoogleCloudDatacatalogV1ListPolicyTagsResponse) error {
				policyTags = append(policyTags, page.PolicyTags...)
				return nil
			},
		)
	})
	if err != nil {
		return "", nil, errors.Wrapf(err, "failed to list the policy tags of taxonomy %s", name)
	}

	return taxonomy.DisplayName, policyTagPaths(policyTags), nil
}

// policyTagPaths gives each policy tag of a taxonomy its display path, from
// the topmost ancestor down. A parent the taxonomy does not list ends the walk
// there, and so does a tree deeper than any Data Catalog allows.
func policyTagPaths(policyTags []*datacatalog.GoogleCloudDatacatalogV1PolicyTag) map[string]string {
	byName := map[string]*datacatalog.GoogleCloudDatacatalogV1PolicyTag{}
	for _, policyTag := range policyTags {
		byName[policyTag.Name] = policyTag
	}
	paths := map[string]string{}
	for _, policyTag := range policyTags {
		var path []string
		for current := policyTag; current != nil && len(path) < maxPolicyTagDepth; current = byName[current.ParentPolicyTag] {
			path = append(path, current.DisplayName)
			if current.ParentPolicyTag == "" {
				break
			}
		}
		for i, j := 0, len(path)-1; i < j; i, j = i+1, j-1 {
			path[i], path[j] = path[j], path[i]
		}
		paths[policyTag.Name] = strings.Join(path, ":")
	}
	return paths
}
