package trino

import "github.com/getsynq/dwhsupport/scrapper/scope"

// ScopeFromConf translates the Trino config's Catalogs field into a ScopeFilter.
// Returns nil if no catalog filtering is configured.
func ScopeFromConf(conf *TrinoScrapperConf) *scope.ScopeFilter {
	if conf == nil || len(conf.Catalogs) == 0 {
		return nil
	}
	rules := make([]scope.ScopeRule, len(conf.Catalogs))
	for i, cat := range conf.Catalogs {
		rules[i] = scope.ScopeRule{Database: cat}
	}
	return &scope.ScopeFilter{Include: rules}
}
