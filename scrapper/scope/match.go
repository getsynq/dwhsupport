package scope

import (
	"regexp"
	"strings"
	"sync"
)

var (
	patternCache   = make(map[string]*regexp.Regexp)
	patternCacheMu sync.RWMutex
)

// compilePattern converts a glob pattern to a compiled regexp.
// * matches zero or more characters. Patterns are anchored and case-insensitive.
func compilePattern(pattern string) *regexp.Regexp {
	patternCacheMu.RLock()
	if re, ok := patternCache[pattern]; ok {
		patternCacheMu.RUnlock()
		return re
	}
	patternCacheMu.RUnlock()

	escaped := regexp.QuoteMeta(pattern)
	regexStr := "(?i)^" + strings.ReplaceAll(escaped, "\\*", ".*") + "$"
	re := regexp.MustCompile(regexStr)

	patternCacheMu.Lock()
	patternCache[pattern] = re
	patternCacheMu.Unlock()

	return re
}

// matchPattern checks if value matches a glob pattern.
// Empty pattern matches everything.
// * in pattern matches zero or more characters. Case-insensitive.
func matchPattern(pattern, value string) bool {
	if pattern == "" {
		return true
	}
	// Fast path: no wildcards — exact case-insensitive comparison.
	if !strings.Contains(pattern, "*") {
		return strings.EqualFold(pattern, value)
	}
	return compilePattern(pattern).MatchString(value)
}

// hasWildcard reports whether the pattern contains a glob wildcard.
func hasWildcard(pattern string) bool {
	return strings.Contains(pattern, "*")
}
