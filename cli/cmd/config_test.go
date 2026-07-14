package cmd

import (
	"testing"
	"time"
)

func TestParseScopeRule(t *testing.T) {
	cases := []struct {
		in              string
		db, schema, tbl string
	}{
		{"analytics", "analytics", "", ""},
		{"analytics.public", "analytics", "public", ""},
		{"analytics.public.orders", "analytics", "public", "orders"},
		{"*.*.stg_*", "*", "*", "stg_*"},
	}
	for _, c := range cases {
		r := parseScopeRule(c.in)
		if r.Database != c.db || r.Schema != c.schema || r.Table != c.tbl {
			t.Errorf("parseScopeRule(%q) = %+v, want {%s %s %s}", c.in, r, c.db, c.schema, c.tbl)
		}
	}
}

func TestEffectiveScope(t *testing.T) {
	if f := effectiveScope(nil, nil, nil); f != nil {
		t.Fatalf("expected nil filter for no scope, got %+v", f)
	}
	f := effectiveScope(nil, []string{"db.public"}, []string{"db.public.tmp_*"})
	if f == nil || len(f.Include) != 1 || len(f.Exclude) != 1 {
		t.Fatalf("unexpected filter: %+v", f)
	}
	if !f.IsObjectAccepted("db", "public", "orders") {
		t.Error("orders should be accepted")
	}
	if f.IsObjectAccepted("db", "public", "tmp_x") {
		t.Error("tmp_x should be excluded")
	}
	if f.IsObjectAccepted("other", "public", "orders") {
		t.Error("other db should not be included")
	}
}

func TestLoadConfigInline(t *testing.T) {
	t.Setenv("PGPASS", "s3cret")
	defer resetConfigFlags()
	resetConfigFlags()
	flagConfigInline = "postgres:\n  host: localhost\n  database: db\n  username: u\n  password: ${PGPASS}\n"

	cfg, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Connection.DialectType() != "postgres" {
		t.Fatalf("dialect = %q", cfg.Connection.DialectType())
	}
	if cfg.Connection.Postgres.Password != "s3cret" {
		t.Fatalf("env not expanded: %q", cfg.Connection.Postgres.Password)
	}
}

func TestLoadConfigNoType(t *testing.T) {
	defer resetConfigFlags()
	resetConfigFlags()
	flagConfigInline = "name: broken\n"
	if _, err := loadConfig(); err == nil {
		t.Fatal("expected error for config without a database type")
	}
}

func TestParseSince(t *testing.T) {
	if tm, err := parseSince(""); err != nil || !tm.IsZero() {
		t.Fatalf("empty should be zero time: %v %v", tm, err)
	}
	tm, err := parseSince("2026-01-02T15:04:05Z")
	if err != nil || tm.Year() != 2026 {
		t.Fatalf("rfc3339 parse: %v %v", tm, err)
	}
	before := time.Now().Add(-25 * time.Hour)
	tm, err = parseSince("24h")
	if err != nil || !tm.After(before) {
		t.Fatalf("duration parse: %v %v", tm, err)
	}
	if _, err := parseSince("7d"); err != nil {
		t.Fatalf("day duration should parse: %v", err)
	}
	if _, err := parseSince("garbage"); err == nil {
		t.Fatal("garbage should error")
	}
}

func TestJqField(t *testing.T) {
	if got := jqField("order_id"); got != "order_id" {
		t.Errorf("simple: %q", got)
	}
	if got := jqField("odd name"); got != `["odd name"]` {
		t.Errorf("quoted: %q", got)
	}
}

func resetConfigFlags() {
	flagConfigPath = ""
	flagConfigInline = ""
	flagNoExpandEnv = false
	flagIncludes = nil
	flagExcludes = nil
}
