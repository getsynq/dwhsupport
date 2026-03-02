package databricks

import (
	"strings"

	"github.com/getsynq/dwhsupport/scrapper/scope"
)

// ScopeFromConf translates the Databricks config's CatalogBlocklist field into a ScopeFilter.
// The blocklist patterns are comma-separated catalog (database) patterns.
// Returns nil if no filtering is configured.
func ScopeFromConf(conf *DatabricksScrapperConf) *scope.ScopeFilter {
	if conf == nil || conf.CatalogBlocklist == "" {
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
