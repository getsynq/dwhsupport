package fabric

import (
	"testing"

	"github.com/pkg/errors"
)

// TestIsQueryInsightsMissing pins the exact-match-then-skip behaviour: only the
// "Invalid object name" error for the queryinsights view (raised for Lakehouse
// SQL endpoints, SQL databases and mirrored databases that have no queryinsights
// schema) marks a database skippable. Every other error — notably permission
// denials — must propagate so real misconfigurations are not silently swallowed.
func TestIsQueryInsightsMissing(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{
			"invalid object name for queryinsights",
			errors.New("mssql: Invalid object name 'COALESCE_DEV_TESTING.queryinsights.exec_requests_history'."),
			true,
		},
		{
			"invalid object name for some other object",
			errors.New("mssql: Invalid object name 'MYDB.dbo.some_table'."),
			false,
		},
		{
			"permission denied on queryinsights must NOT skip",
			errors.New(
				"mssql: The SELECT permission or external policy action 'Microsoft.Sql/.../Select' was denied on the object 'exec_requests_history', database 'COALESCE_QUALITY_DWHTESTING', schema 'queryinsights'.",
			),
			false,
		},
		{
			"unrelated error",
			errors.New("mssql: Login failed"),
			false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isQueryInsightsMissing(tc.err); got != tc.want {
				t.Fatalf("isQueryInsightsMissing(%q) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
