package scope

// Merge combines multiple filters into one.
// The merged filter accepts a value only if it passes ALL input filters (AND semantics).
//
// For include rules across filters: a value must match at least one include from each
// filter that has includes (AND of ORs).
// For exclude rules across filters: a value matched by any exclude from any filter is rejected.
//
// Nil filters are skipped.
func Merge(filters ...*ScopeFilter) *ScopeFilter {
	var nonNil []*ScopeFilter
	for _, f := range filters {
		if f != nil && !f.isEmpty() {
			nonNil = append(nonNil, f)
		}
	}
	if len(nonNil) == 0 {
		return nil
	}
	if len(nonNil) == 1 {
		return nonNil[0]
	}
	return &ScopeFilter{children: nonNil}
}

func (f *ScopeFilter) isEmpty() bool {
	return f == nil || (len(f.Include) == 0 && len(f.Exclude) == 0 && len(f.children) == 0)
}
