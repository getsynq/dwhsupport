// Package yamlconfig provides YAML-based DWH connection config parsing and proto conversion.
package yamlconfig

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// ParseOptions controls how YAML config is parsed.
type ParseOptions struct {
	// ExpandEnv expands ${VAR} references in YAML scalar values.
	// When true and LookupVar is nil, os.Getenv is used.
	// When true and LookupVar is set, LookupVar is used instead.
	ExpandEnv bool

	// LookupVar resolves variable names to values during ${VAR} expansion.
	// Called with the variable name (without ${}); returns the value and whether it was found.
	// When nil and ExpandEnv is true, os.LookupEnv is used.
	// Use this to read variables from a secrets manager instead of the process environment.
	LookupVar func(name string) (string, bool)

	// StrictEnv makes expansion fail when a referenced variable is not found.
	// Only applies when ExpandEnv is true. Unset variables in ${VAR} syntax cause
	// a parse error. Use ${VAR:-default} to provide a fallback, or ${VAR:-} for empty string.
	StrictEnv bool

	// BaseDir is the directory against which relative file paths are resolved.
	// Typically filepath.Dir(configPath). Only used when ReadFile is set.
	BaseDir string

	// ReadFile reads a file at the given absolute path. Used to resolve *_file fields
	// (e.g., private_key_file, service_account_key_file).
	// When nil, file fields are left as-is (paths remain as strings, inline fields are not populated).
	// When set, relative paths are resolved against BaseDir before calling ReadFile.
	// Cloud deployments can pass a function that returns an error to prevent file access.
	ReadFile func(path string) ([]byte, error)
}

// CLIOptions returns ParseOptions suitable for CLI tools: env expansion enabled,
// file reading via os.ReadFile, paths resolved relative to the config file.
func CLIOptions(configPath string) ParseOptions {
	return ParseOptions{
		ExpandEnv: true,
		BaseDir:   filepath.Dir(configPath),
		ReadFile:  os.ReadFile,
	}
}

// Parse unmarshals YAML data into the target struct.
// If opts.ExpandEnv is true, ${VAR} references in scalar values are expanded.
func Parse(data []byte, target any, opts ParseOptions) error {
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return fmt.Errorf("yamlconfig: parsing YAML: %w", err)
	}

	if opts.ExpandEnv {
		lookup := opts.LookupVar
		if lookup == nil {
			lookup = os.LookupEnv
		}
		if err := expandEnvNodes(&doc, lookup, opts.StrictEnv); err != nil {
			return err
		}
	}

	if err := doc.Decode(target); err != nil {
		return fmt.Errorf("yamlconfig: decoding YAML: %w", err)
	}

	return nil
}

// ParseConnections parses a YAML document and extracts the connections map.
// The YAML must have a top-level "connections" key.
func ParseConnections(data []byte, opts ParseOptions) (map[string]*Connection, error) {
	var wrapper struct {
		Connections map[string]*Connection `yaml:"connections"`
	}
	if err := Parse(data, &wrapper, opts); err != nil {
		return nil, err
	}
	if wrapper.Connections == nil {
		return nil, fmt.Errorf("yamlconfig: no connections found in config")
	}
	if err := ResolveFiles(wrapper.Connections, opts); err != nil {
		return nil, err
	}
	return wrapper.Connections, nil
}

// ParseConnectionsOnly parses a YAML document where the top-level keys are
// connection IDs directly (no wrapping "connections" key).
func ParseConnectionsOnly(data []byte, opts ParseOptions) (map[string]*Connection, error) {
	var conns map[string]*Connection
	if err := Parse(data, &conns, opts); err != nil {
		return nil, err
	}
	if err := ResolveFiles(conns, opts); err != nil {
		return nil, err
	}
	return conns, nil
}

// expandEnvNodes recursively expands ${VAR} and ${VAR:-default} references in YAML
// scalar node values using the provided lookup function.
//
// Supported syntax:
//   - ${VAR}          — replaced with lookup result; empty string if not found (or error in strict mode)
//   - ${VAR:-default} — replaced with lookup result if found, otherwise "default"
//   - ${VAR:-}        — replaced with lookup result if found, otherwise empty string (never fails in strict mode)
//
// After expansion, the node tag is cleared so yaml.v3 re-infers the type from the
// expanded value (e.g., "5432" → int when the target field is int).
func expandEnvNodes(node *yaml.Node, lookup func(string) (string, bool), strict bool) error {
	if node == nil {
		return nil
	}
	if node.Kind == yaml.ScalarNode {
		var expandErr error
		expanded := os.Expand(node.Value, func(key string) string {
			// Handle ${VAR:-default} syntax
			name, defaultVal, hasDefault := parseVarDefault(key)

			val, found := lookup(name)
			if found {
				return val
			}
			if hasDefault {
				return defaultVal
			}
			if strict {
				expandErr = fmt.Errorf("yamlconfig: variable %q not found", name)
			}
			return ""
		})
		if expandErr != nil {
			return expandErr
		}
		if expanded != node.Value {
			node.Value = expanded
			// Clear the tag so yaml.v3 re-infers the type from the new value.
			// Without this, a value like ${PORT} that was tagged !!str would
			// remain a string even after expanding to "5432".
			node.Tag = ""
		}
		return nil
	}
	for _, child := range node.Content {
		if err := expandEnvNodes(child, lookup, strict); err != nil {
			return err
		}
	}
	return nil
}

// parseVarDefault splits "VAR:-default" into (name, default, hasDefault).
// For plain "VAR", returns (VAR, "", false).
func parseVarDefault(key string) (string, string, bool) {
	if idx := strings.Index(key, ":-"); idx >= 0 {
		return key[:idx], key[idx+2:], true
	}
	return key, "", false
}

// fileFieldPair associates a file path field with the inline content field it populates.
type fileFieldPair struct {
	fileField   *string
	inlineField *string
}

// ResolveFiles reads files referenced by *_file fields and populates the corresponding
// inline fields. Only acts when opts.ReadFile is non-nil.
func ResolveFiles(connections map[string]*Connection, opts ParseOptions) error {
	if opts.ReadFile == nil {
		return nil
	}

	for id, conn := range connections {
		pairs := collectFileFields(conn)
		for _, p := range pairs {
			if *p.fileField == "" {
				continue
			}
			// If inline field already has content, skip (explicit value takes precedence)
			if *p.inlineField != "" {
				continue
			}
			absPath := *p.fileField
			if !filepath.IsAbs(absPath) {
				absPath = filepath.Join(opts.BaseDir, absPath)
			}
			data, err := opts.ReadFile(absPath)
			if err != nil {
				return fmt.Errorf("yamlconfig: connection %q: reading file %q: %w", id, *p.fileField, err)
			}
			*p.inlineField = string(data)
			// Clear the _file field so downstream proto validators that enforce
			// "either inline or file, but not both" don't reject the config.
			*p.fileField = ""
		}
	}
	return nil
}

// ApplyDefaults sets default values on parsed connections.
// Currently sets Parallelism to 8 when not explicitly configured.
func ApplyDefaults(conns map[string]*Connection) {
	for _, conn := range conns {
		if conn.Parallelism == 0 {
			conn.Parallelism = 8
		}
	}
}

// collectFileFields returns all file field pairs for a connection.
func collectFileFields(conn *Connection) []fileFieldPair {
	var pairs []fileFieldPair
	if conn.Snowflake != nil {
		pairs = append(pairs, conn.Snowflake.fileFields()...)
	}
	if conn.BigQuery != nil {
		pairs = append(pairs, conn.BigQuery.fileFields()...)
	}
	return pairs
}
