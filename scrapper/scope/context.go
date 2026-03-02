package scope

import "context"

type scopeChainKey struct{}

// WithScope appends a filter to the scope chain in context.
// Multiple calls stack filters — a value must pass ALL filters (AND).
// If filter is nil, returns ctx unchanged.
func WithScope(ctx context.Context, filter *ScopeFilter) context.Context {
	if filter == nil {
		return ctx
	}
	chain := getScopeChain(ctx)
	newChain := make([]*ScopeFilter, len(chain)+1)
	copy(newChain, chain)
	newChain[len(chain)] = filter
	return context.WithValue(ctx, scopeChainKey{}, newChain)
}

// GetScope returns the effective composed filter from context.
// Returns nil if no scope is set.
func GetScope(ctx context.Context) *ScopeFilter {
	chain := getScopeChain(ctx)
	if len(chain) == 0 {
		return nil
	}
	if len(chain) == 1 {
		return chain[0]
	}
	return Merge(chain...)
}

func getScopeChain(ctx context.Context) []*ScopeFilter {
	chain, _ := ctx.Value(scopeChainKey{}).([]*ScopeFilter)
	return chain
}
