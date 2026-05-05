package athena

import "github.com/getsynq/dwhsupport/scrapper/scope"

// ScopeFromConf is a placeholder — the cloud integration translates the
// proto's ScopeFilter directly into scope.ScopeFilter. Athena's scrapper
// has no legacy filter fields to translate.
func ScopeFromConf(_ *AthenaScrapperConf) *scope.ScopeFilter { return nil }
