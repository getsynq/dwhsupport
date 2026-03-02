package snowflake

import "github.com/getsynq/dwhsupport/scrapper/scope"

// ScopeFromConf translates the Snowflake config's Databases field into a ScopeFilter.
// Returns nil if no database filtering is configured.
func ScopeFromConf(conf *SnowflakeScrapperConf) *scope.ScopeFilter {
	if conf == nil || len(conf.Databases) == 0 {
		return nil
	}
	rules := make([]scope.ScopeRule, len(conf.Databases))
	for i, db := range conf.Databases {
		rules[i] = scope.ScopeRule{Database: db}
	}
	return &scope.ScopeFilter{Include: rules}
}
