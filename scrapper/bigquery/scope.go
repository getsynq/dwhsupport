package bigquery

import (
	"strings"

	"github.com/getsynq/dwhsupport/scrapper/scope"
)

// ScopeFromConf translates the BigQuery config's Datasets allowlist and
// Blocklist into a ScopeFilter.
//
//   - Datasets (allowlist) maps to Include rules at the schema level.
//   - Blocklist (comma-separated patterns) maps to Exclude rules at the schema level.
//
// Returns nil if neither field is configured — callers treat nil as accept-all.
func ScopeFromConf(conf *BigQueryScrapperConf) *scope.ScopeFilter {
	if conf == nil {
		return nil
	}

	var include []scope.ScopeRule
	for _, ds := range conf.Datasets {
		ds = strings.TrimSpace(ds)
		if ds != "" {
			include = append(include, scope.ScopeRule{Schema: ds})
		}
	}

	var exclude []scope.ScopeRule
	if conf.Blocklist != "" {
		for _, p := range strings.Split(conf.Blocklist, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				exclude = append(exclude, scope.ScopeRule{Schema: p})
			}
		}
	}

	if len(include) == 0 && len(exclude) == 0 {
		return nil
	}
	return &scope.ScopeFilter{Include: include, Exclude: exclude}
}
