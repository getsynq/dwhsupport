// Package dbtprofiles parses dbt profiles.yml files into yamlconfig.Connection
// or agentdwhv1.Connection types.
package dbtprofiles

import (
	"fmt"
	"os"
	"regexp"
	"strings"

	agentdwhv1 "buf.build/gen/go/getsynq/api/protocolbuffers/go/synq/agent/dwh/v1"
	"github.com/getsynq/dwhsupport/yamlconfig"
	"gopkg.in/yaml.v3"
)

// profiles is the top-level structure of a dbt profiles.yml file.
type profiles map[string]profile

type profile struct {
	Target  string                    `yaml:"target"`
	Outputs map[string]map[string]any `yaml:"outputs"`
}

// ResolveProfilesPath returns the path to the dbt profiles.yml file,
// following dbt's standard search order:
//  1. $DBT_PROFILES_DIR/profiles.yml
//  2. ./profiles.yml (current working directory)
//  3. ~/.dbt/profiles.yml
func ResolveProfilesPath() string {
	if dir := os.Getenv("DBT_PROFILES_DIR"); dir != "" {
		return dir + "/profiles.yml"
	}
	if _, err := os.Stat("profiles.yml"); err == nil {
		return "profiles.yml"
	}
	home, _ := os.UserHomeDir()
	return home + "/.dbt/profiles.yml"
}

// LoadConnections parses a dbt profiles.yml and returns YAML connections.
// Each profile/target pair becomes a connection. Connection names use the format
// "profile" (default target) or "profile/target".
func LoadConnections(path string) (map[string]*yamlconfig.Connection, error) {
	profs, err := loadProfiles(path)
	if err != nil {
		return nil, err
	}

	conns := make(map[string]*yamlconfig.Connection)
	for profileName, prof := range profs {
		for targetName, output := range prof.Outputs {
			conn, err := outputToConnection(output)
			if err != nil {
				return nil, fmt.Errorf("dbtprofiles: profile %q target %q: %w", profileName, targetName, err)
			}
			// Use "profile/target" as connection ID
			connID := profileName + "/" + targetName
			conn.Name = profileName + " (" + targetName + ")"
			conns[connID] = conn

			// Also register as just "profile" if this is the default target
			if targetName == prof.Target {
				conns[profileName] = conn
			}
		}
	}
	return conns, nil
}

// LoadProtoConnections parses a dbt profiles.yml and returns proto Connection messages.
func LoadProtoConnections(path string) (map[string]*agentdwhv1.Connection, error) {
	conns, err := LoadConnections(path)
	if err != nil {
		return nil, err
	}
	return yamlconfig.ToProtoConnections(conns)
}

func loadProfiles(path string) (profiles, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("dbtprofiles: reading %s: %w", path, err)
	}

	// Resolve {{ env_var('NAME') }} and {{ env_var('NAME', 'default') }}
	resolved := resolveEnvVars(string(data))

	var profs profiles
	if err := yaml.Unmarshal([]byte(resolved), &profs); err != nil {
		return nil, fmt.Errorf("dbtprofiles: parsing %s: %w", path, err)
	}
	return profs, nil
}

var envVarRe = regexp.MustCompile(`\{\{\s*env_var\(\s*['"]([^'"]+)['"]\s*(?:,\s*['"]([^'"]*)['"]\s*)?\)\s*\}\}`)

// resolveEnvVars replaces {{ env_var('NAME') }} and {{ env_var('NAME', 'default') }}
// with the corresponding environment variable value.
func resolveEnvVars(s string) string {
	return envVarRe.ReplaceAllStringFunc(s, func(match string) string {
		parts := envVarRe.FindStringSubmatch(match)
		if len(parts) < 2 {
			return match
		}
		name := parts[1]
		if val, ok := os.LookupEnv(name); ok {
			return val
		}
		if len(parts) >= 3 && parts[2] != "" {
			return parts[2]
		}
		return match
	})
}

func str(m map[string]any, key string) string {
	v, _ := m[key].(string)
	return v
}

func intVal(m map[string]any, key string, fallback int) int {
	switch v := m[key].(type) {
	case int:
		return v
	case float64:
		return int(v)
	}
	return fallback
}

func boolVal(m map[string]any, key string) bool {
	v, _ := m[key].(bool)
	return v
}

func outputToConnection(output map[string]any) (*yamlconfig.Connection, error) {
	dbType := str(output, "type")
	conn := &yamlconfig.Connection{}

	switch strings.ToLower(dbType) {
	case "postgres", "postgresql":
		conn.Postgres = &yamlconfig.PostgresConf{
			Host:     str(output, "host"),
			Port:     intVal(output, "port", 5432),
			Username: str(output, "user"),
			Password: str(output, "password"),
			Database: str(output, "dbname"),
		}

	case "redshift":
		conn.Redshift = &yamlconfig.RedshiftConf{
			Host:     str(output, "host"),
			Port:     intVal(output, "port", 5439),
			Username: str(output, "user"),
			Password: str(output, "password"),
			Database: str(output, "dbname"),
		}

	case "snowflake":
		sf := &yamlconfig.SnowflakeConf{
			Account:   str(output, "account"),
			Username:  str(output, "user"),
			Password:  str(output, "password"),
			Warehouse: str(output, "warehouse"),
			Role:      str(output, "role"),
		}
		if db := str(output, "database"); db != "" {
			sf.Databases = []string{db}
		}
		if keyfile := str(output, "private_key_path"); keyfile != "" {
			sf.PrivateKeyFile = keyfile
		}
		if passphrase := str(output, "private_key_passphrase"); passphrase != "" {
			sf.PrivateKeyPassphrase = passphrase
		}
		conn.Snowflake = sf

	case "bigquery":
		bq := &yamlconfig.BigQueryConf{
			ProjectId: str(output, "project"),
			Region:    str(output, "location"),
		}
		if keyfile := str(output, "keyfile"); keyfile != "" {
			bq.ServiceAccountKeyFile = keyfile
		}
		if keyJSON := str(output, "keyfile_json"); keyJSON != "" {
			bq.ServiceAccountKey = keyJSON
		}
		conn.BigQuery = bq

	case "databricks":
		dbr := &yamlconfig.DatabricksConf{
			WorkspaceUrl: "https://" + str(output, "host"),
		}
		if httpPath := str(output, "http_path"); httpPath != "" {
			dbr.Warehouse = httpPath
		}
		if token := str(output, "token"); token != "" {
			dbr.AuthToken = token
		}
		conn.Databricks = dbr

	case "clickhouse":
		conn.Clickhouse = &yamlconfig.ClickhouseConf{
			Host:     str(output, "host"),
			Port:     intVal(output, "port", 9440),
			Username: str(output, "user"),
			Password: str(output, "password"),
			Database: str(output, "database"),
		}

	case "trino", "presto":
		conn.Trino = &yamlconfig.TrinoConf{
			Host:     str(output, "host"),
			Port:     intVal(output, "port", 443),
			Username: str(output, "user"),
			Password: str(output, "password"),
		}

	case "mysql":
		conn.MySQL = &yamlconfig.MySQLConf{
			Host:     str(output, "host"),
			Port:     intVal(output, "port", 3306),
			Username: str(output, "user"),
			Password: str(output, "password"),
			Database: str(output, "database"),
		}

	case "duckdb":
		db := str(output, "path")
		if db == "" {
			db = ":memory:"
		}
		conn.DuckDB = &yamlconfig.DuckDBConf{
			Database: db,
		}

	case "oracle":
		conn.Oracle = &yamlconfig.OracleConf{
			Host:        str(output, "host"),
			Port:        intVal(output, "port", 1521),
			ServiceName: str(output, "service"),
			Username:    str(output, "user"),
			Password:    str(output, "password"),
			SSL:         boolVal(output, "ssl"),
		}

	case "sqlserver", "mssql":
		conn.MSSQL = &yamlconfig.MSSQLConf{
			Host:     str(output, "host"),
			Port:     intVal(output, "port", 1433),
			Database: str(output, "database"),
			Username: str(output, "user"),
			Password: str(output, "password"),
		}

	default:
		return nil, fmt.Errorf("unsupported dbt connection type %q", dbType)
	}

	return conn, nil
}
