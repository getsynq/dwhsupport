package databricks

import (
	"context"
	"strings"

	"github.com/getsynq/dwhsupport/scrapper/scope"
)

// ScopeFromConf resolves the scope the config asks for.
//
// Scope is what callers should set. CatalogBlocklist is the older, narrower way of
// saying the same thing — comma-separated catalog patterns, exclusions only — and is
// read only when Scope is unset, so a caller that has migrated is never also subject
// to a stale blocklist. A Scope that is present but carries no rules therefore means
// "no filtering", not "fall back to the blocklist".
//
// Returns nil when neither is configured, which every ScopeFilter method treats as
// accepting everything.
func ScopeFromConf(conf *DatabricksScrapperConf) *scope.ScopeFilter {
	if conf == nil {
		return nil
	}
	if conf.Scope != nil {
		return conf.Scope
	}
	if conf.CatalogBlocklist == "" {
		return nil
	}
	patterns := strings.Split(conf.CatalogBlocklist, ",")
	var rules []scope.ScopeRule
	for _, p := range patterns {
		p = strings.TrimSpace(p)
		if p != "" {
			rules = append(rules, scope.ScopeRule{Database: p})
		}
	}
	if len(rules) == 0 {
		return nil
	}
	return &scope.ScopeFilter{Exclude: rules}
}

// effectiveScope is the scope a walk in progress has to respect: what the config
// configured, and whatever the caller of this particular scrape asked for on top
// (scope.WithScope). Both apply — a per-call scope narrows the configured one rather
// than replacing it.
//
// Every walk of Unity Catalog filters through this rather than through e.scope
// directly, so a per-call scope skips catalogs and schemas before they are listed
// instead of only having their rows discarded afterwards. Correctness does not depend
// on it — ScopedScrapper post-filters what comes back — but a catalog that is out of
// scope is worth not walking at all.
func (e *DatabricksScrapper) effectiveScope(ctx context.Context) *scope.ScopeFilter {
	return scope.Merge(e.scope, scope.GetScope(ctx))
}
