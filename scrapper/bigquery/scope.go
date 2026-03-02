package bigquery

import (
	"strings"

	"github.com/getsynq/dwhsupport/scrapper/scope"
)

// ScopeFromConf translates the BigQuery config's Blocklist field into a ScopeFilter.
// Blocklist patterns are comma-separated dataset (schema) patterns.
// Returns nil if no filtering is configured.
func ScopeFromConf(conf *BigQueryScrapperConf) *scope.ScopeFilter {
	if conf == nil || conf.Blocklist == "" {
		return nil
	}
	patterns := strings.Split(conf.Blocklist, ",")
	var rules []scope.ScopeRule
	for _, p := range patterns {
		p = strings.TrimSpace(p)
		if p != "" {
			rules = append(rules, scope.ScopeRule{Schema: p})
		}
	}
	if len(rules) == 0 {
		return nil
	}
	return &scope.ScopeFilter{Exclude: rules}
}
