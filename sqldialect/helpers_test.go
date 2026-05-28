package sqldialect

import "testing"

func TestIsUpper(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"", true},
		{"FOO", true},
		{"FOO_BAR", true},
		{"FOO123", true},
		{"foo", false},
		{"Foo", false},
		{"123", true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			if got := IsUpper(tc.in); got != tc.want {
				t.Errorf("IsUpper(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestQuoteForFoldUpper(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"ingested_at", "ingested_at"},
		{"INGESTED_AT", "INGESTED_AT"},
		{"ingestedAt", `"ingestedAt"`},
		{"Created At", `"Created At"`},
		{"_meta/mtime", `"_meta/mtime"`},
		{"a\"b", `"a""b"`},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			if got := QuoteForFoldUpper(tc.in, `"`); got != tc.want {
				t.Errorf("QuoteForFoldUpper(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestQuoteForFoldLower(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"ingested_at", "ingested_at"},
		{"INGESTED_AT", `"INGESTED_AT"`},
		{"ingestedAt", `"ingestedAt"`},
		{"Created At", `"Created At"`},
		{"_meta/mtime", `"_meta/mtime"`},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			if got := QuoteForFoldLower(tc.in, `"`); got != tc.want {
				t.Errorf("QuoteForFoldLower(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestQuoteWithBracketsIfNeeded(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"ingested_at", "ingested_at"},
		{"INGESTED_AT", "INGESTED_AT"},
		{"Created At", "[Created At]"},
		{"with]bracket", "[with]]bracket]"},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			if got := QuoteWithBracketsIfNeeded(tc.in); got != tc.want {
				t.Errorf("QuoteWithBracketsIfNeeded(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestIsLikelyExpression(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"ingested_at", false},
		{"Created At", false},
		{"_meta/mtime", false},
		{"lower(name)", true},
		{"coalesce(a, b)", true},
		{"value::numeric", true},
		{"payload->>'amount'", true},
		{"data->'nested'", true},
		{"a #>> '{b,c}'", true},
		{"CAST(x AS INT)", true},
		{"foo as bar", true},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			if got := isLikelyExpression(tc.in); got != tc.want {
				t.Errorf("isLikelyExpression(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}
