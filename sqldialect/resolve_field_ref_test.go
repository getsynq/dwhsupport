package sqldialect

import "testing"

func TestResolveFieldRef_Snowflake(t *testing.T) {
	d := NewSnowflakeDialect()
	cases := []struct {
		in, want string
	}{
		{"ingested_at", "ingested_at"},
		{"INGESTED_AT", "INGESTED_AT"},
		{"ingestedAt", `"ingestedAt"`},
		{"Created At", `"Created At"`},
		{"_meta/mtime", `"_meta/mtime"`},
		{"payload->>'amount'", "payload->>'amount'"},
		{"value::numeric", "value::numeric"},
		{"CAST(x AS INT)", "CAST(x AS INT)"},
		{"lower(name)", "lower(name)"},
		{"coalesce(a, b)", "coalesce(a, b)"},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			if got := d.ResolveFieldRef(tc.in); got != tc.want {
				t.Errorf("Snowflake.ResolveFieldRef(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}
