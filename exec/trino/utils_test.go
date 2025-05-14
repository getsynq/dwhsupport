package trino

import "testing"

func TestRemoveTrailingSemicolon(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"SELECT 1;", "SELECT 1"},
		{"SELECT 1", "SELECT 1"},
		{";", ""},
		{"", ""},
		{"SELECT 1;;", "SELECT 1"},
	}

	for _, tt := range tests {
		got := trimRightSemicolons(tt.input)
		if got != tt.expected {
			t.Errorf("removeTrailingSemicolon(%q) = %q; want %q", tt.input, got, tt.expected)
		}
	}
}
