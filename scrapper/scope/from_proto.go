package scope

import (
	commonv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/common/v1"
)

// FromProto converts a synq.common.v1.ScopeFilter into the runtime
// *ScopeFilter used by the scrapper packages. Returns nil for a nil input
// or one with no rules — both mean accept-all.
func FromProto(p *commonv1.ScopeFilter) *ScopeFilter {
	if p == nil {
		return nil
	}
	if len(p.GetInclude()) == 0 && len(p.GetExclude()) == 0 {
		return nil
	}
	out := &ScopeFilter{
		Include: make([]ScopeRule, 0, len(p.GetInclude())),
		Exclude: make([]ScopeRule, 0, len(p.GetExclude())),
	}
	for _, r := range p.GetInclude() {
		out.Include = append(out.Include, ScopeRule{
			Database: r.GetDatabase(),
			Schema:   r.GetSchema(),
			Table:    r.GetTable(),
		})
	}
	for _, r := range p.GetExclude() {
		out.Exclude = append(out.Exclude, ScopeRule{
			Database: r.GetDatabase(),
			Schema:   r.GetSchema(),
			Table:    r.GetTable(),
		})
	}
	return out
}
