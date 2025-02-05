package blocklist

import (
	"strings"
	"testing"
)

func TestNewBlocklist(t *testing.T) {

	cases := []struct {
		patterns []string
		accepted []string
		rejected []string
	}{
		{
			patterns: []string{"test_*"},
			accepted: []string{"something.bar", "tests_bar.baz.bar", "tests_bar"},
			rejected: []string{"test_bar.baz", "test_bar.baz.bar.baz"},
		},
		{
			patterns: []string{"*.pr_*"},
			accepted: []string{"test_prep.pr123", "test_prep.pr15987_dwh_seed_finance.tbl"},
			rejected: []string{"test_prep.pr_15987_dwh_seed_finance.tbl"},
		},
		{
			patterns: []string{"..."},
			accepted: []string{"foo"},
		},
	}

	for _, test := range cases {

		t.Run(strings.Join(test.patterns, ","), func(t *testing.T) {
			bl := NewBlocklist(test.patterns)
			for _, str := range test.accepted {
				if bl.IsBlocked(str) {
					t.Errorf("expected %s to be accepted", str)
				}
			}
			for _, str := range test.rejected {
				if !bl.IsBlocked(str) {
					t.Errorf("expected %s to be rejected", str)
				}
			}
		})

	}

}
