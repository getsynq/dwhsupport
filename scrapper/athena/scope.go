package athena

import "github.com/getsynq/dwhsupport/scrapper/scope"

// ScopeFromConf returns the configured ScopeFilter as-is. Athena is the first
// warehouse to use synq.common.v1.ScopeFilter directly on both internal and
// agent protos (no legacy `repeated string databases` field to translate
// from), so this is just a passthrough — kept for symmetry with the other
// scrappers' ScopeFromConf functions.
//
// Mapping note: Athena's hierarchy is Glue catalog → Glue database → table.
// In ScopeFilter terms (mirroring BigQuery's project/dataset shape):
//
//	ScopeRule.database = Glue catalog (almost always 'AwsDataCatalog')
//	ScopeRule.schema   = Glue database
//	ScopeRule.table    = Glue table / view
func ScopeFromConf(conf *AthenaScrapperConf) *scope.ScopeFilter {
	if conf == nil {
		return nil
	}
	return conf.Scope
}
