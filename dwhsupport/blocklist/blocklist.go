package blocklist

import (
	"github.com/samber/lo"
	"regexp"
	"strings"
)

type Blocklist interface {
	IsBlocked(str string) bool
}

type BlocklistImpl struct {
	patterns []*regexp.Regexp
}

func NewEmptyBlocklist() Blocklist {
	return &BlocklistImpl{}
}

func NewBlocklistFromString(commaSeparatedBlocklist string) Blocklist {
	blocklist := lo.WithoutEmpty(lo.Map(strings.Split(commaSeparatedBlocklist, ","), func(pattern string, _ int) string {
		return strings.TrimSpace(pattern)
	}))
	return NewBlocklist(blocklist)
}

func NewBlocklist(patterns []string) Blocklist {
	regexPatterns := make([]*regexp.Regexp, 0, len(patterns))
	for _, pattern := range patterns {
		if pattern == "" {
			continue
		}
		escapedPattern := regexp.QuoteMeta(pattern)
		regexPattern := "^" + strings.ReplaceAll(escapedPattern, "\\*", ".+") + "$"
		regexPatterns = append(regexPatterns, regexp.MustCompile(regexPattern))
	}

	return &BlocklistImpl{patterns: regexPatterns}
}

func (b *BlocklistImpl) IsBlocked(str string) bool {
	for _, regexPattern := range b.patterns {
		if regexPattern.MatchString(str) {
			return true
		}
	}

	return false
}
