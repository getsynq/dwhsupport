package cmd

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/getsynq/dwhsupport/connect"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/getsynq/dwhsupport/scrapper/scope"
	"github.com/getsynq/dwhsupport/yamlconfig"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// Connection config is provided through one of these, in precedence order:
//  1. --config-inline  (raw YAML or JSON string)
//  2. --config <path>  ("-" reads stdin)
//  3. $DWHCTL_CONFIG   (a file path)
//
// The document is a single connection keyed by its dialect, optionally with a
// top-level scope filter, e.g.:
//
//	snowflake:
//	  account: ab12345.eu-west-1
//	  username: SVC_READONLY
//	  password: ${SF_PASSWORD}
//	  warehouse: COMPUTE_WH
//	scope:
//	  include:
//	    - database: ANALYTICS
//	  exclude:
//	    - schema: STAGING
//
// ${VAR} references are expanded from the environment (disable with
// --no-expand-env), and *_file fields (private_key_file, service_account_key_file)
// are read relative to the config file.
var (
	flagConfigPath   string
	flagConfigInline string
	flagNoExpandEnv  bool
	flagIncludes     []string
	flagExcludes     []string
)

const configEnvVar = "DWHCTL_CONFIG"

// cliConfig is a single connection plus an optional universal scope filter.
// Connection is inlined so the document matches a `connections:` entry from a
// synq-dwh config, making configs portable between the two tools.
type cliConfig struct {
	Connection yamlconfig.Connection `yaml:",inline"`
	Scope      *yamlconfig.ScopeConf `yaml:"scope,omitempty"`
}

func registerConnectionFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().
		StringVarP(&flagConfigPath, "config", "c", "", "Path to connection config (YAML or JSON); use '-' for stdin. Falls back to $"+configEnvVar)
	cmd.PersistentFlags().StringVar(&flagConfigInline, "config-inline", "", "Connection config as an inline YAML or JSON string")
	cmd.PersistentFlags().BoolVar(&flagNoExpandEnv, "no-expand-env", false, "Do not expand ${VAR} references in the config")
	cmd.PersistentFlags().
		StringArrayVar(&flagIncludes, "include", nil, "Scope include pattern 'database[.schema[.table]]' (repeatable; * is a wildcard)")
	cmd.PersistentFlags().
		StringArrayVar(&flagExcludes, "exclude", nil, "Scope exclude pattern 'database[.schema[.table]]' (repeatable; * is a wildcard)")
}

// loadConfig reads and parses the connection config from the configured source.
func loadConfig() (*cliConfig, error) {
	data, baseDir, err := readConfigBytes()
	if err != nil {
		return nil, err
	}

	opts := yamlconfig.ParseOptions{
		ExpandEnv: !flagNoExpandEnv,
		BaseDir:   baseDir,
		ReadFile:  os.ReadFile,
	}

	var cfg cliConfig
	if err := yamlconfig.Parse(data, &cfg, opts); err != nil {
		return nil, err
	}
	if cfg.Connection.DialectType() == "" {
		return nil, errors.New("no database type configured: the config must set exactly one of snowflake, bigquery, postgres, ... at the top level")
	}
	// Resolve *_file fields (private_key_file, service_account_key_file, ...).
	conns := map[string]*yamlconfig.Connection{"connection": &cfg.Connection}
	if err := yamlconfig.ResolveFiles(conns, opts); err != nil {
		return nil, err
	}
	return &cfg, nil
}

func readConfigBytes() (data []byte, baseDir string, err error) {
	switch {
	case flagConfigInline != "":
		return []byte(flagConfigInline), ".", nil
	case flagConfigPath == "-":
		b, err := io.ReadAll(os.Stdin)
		if err != nil {
			return nil, "", errors.Wrap(err, "reading config from stdin")
		}
		return b, ".", nil
	case flagConfigPath != "":
		b, err := os.ReadFile(flagConfigPath)
		if err != nil {
			return nil, "", errors.Wrap(err, "reading config file")
		}
		return b, filepath.Dir(flagConfigPath), nil
	}
	if envPath := os.Getenv(configEnvVar); envPath != "" {
		b, err := os.ReadFile(envPath)
		if err != nil {
			return nil, "", errors.Wrapf(err, "reading config file from $%s", configEnvVar)
		}
		return b, filepath.Dir(envPath), nil
	}
	return nil, "", errors.New(
		"no connection config provided: pass --config <file>, --config-inline <yaml/json>, pipe it via --config -, or set $" + configEnvVar,
	)
}

// buildScrapper loads the config, connects to the warehouse, and wraps the
// scrapper with the effective scope filter (config scope AND-ed with any
// --include/--exclude flags). The caller must Close the returned scrapper.
func buildScrapper(ctx context.Context) (scrapper.Scrapper, error) {
	cfg, err := loadConfig()
	if err != nil {
		return nil, err
	}

	proto, err := yamlconfig.ToProtoConnection("connection", &cfg.Connection)
	if err != nil {
		return nil, err
	}

	s, err := connect.Connect(ctx, proto)
	if err != nil {
		return nil, errors.Wrap(err, "failed to connect to the data warehouse")
	}

	if filter := effectiveScope(cfg.Scope, flagIncludes, flagExcludes); filter != nil {
		s = scope.NewScopedScrapper(s, filter)
	}
	return s, nil
}

// effectiveScope merges the config-level scope with CLI --include/--exclude
// patterns into a single runtime filter, or returns nil when nothing is scoped.
func effectiveScope(sc *yamlconfig.ScopeConf, includes, excludes []string) *scope.ScopeFilter {
	f := &scope.ScopeFilter{}
	if sc != nil {
		for _, r := range sc.Include {
			f.Include = append(f.Include, scope.ScopeRule{Database: r.Database, Schema: r.Schema, Table: r.Table})
		}
		for _, r := range sc.Exclude {
			f.Exclude = append(f.Exclude, scope.ScopeRule{Database: r.Database, Schema: r.Schema, Table: r.Table})
		}
	}
	for _, s := range includes {
		f.Include = append(f.Include, parseScopeRule(s))
	}
	for _, s := range excludes {
		f.Exclude = append(f.Exclude, parseScopeRule(s))
	}
	if len(f.Include) == 0 && len(f.Exclude) == 0 {
		return nil
	}
	return f
}

// parseScopeRule turns a dotted 'database[.schema[.table]]' pattern into a
// ScopeRule. Missing trailing levels are left empty (wildcard). A literal '*'
// at a level also matches anything.
func parseScopeRule(s string) scope.ScopeRule {
	parts := strings.SplitN(s, ".", 3)
	var r scope.ScopeRule
	if len(parts) > 0 {
		r.Database = parts[0]
	}
	if len(parts) > 1 {
		r.Schema = parts[1]
	}
	if len(parts) > 2 {
		r.Table = parts[2]
	}
	return r
}
