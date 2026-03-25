package yamlconfig

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// ParseOptions controls how YAML config is parsed.
type ParseOptions struct {
	// ExpandEnv expands ${VAR} references in YAML scalar values using the process environment.
	ExpandEnv bool

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
		expandEnvNodes(&doc)
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

// expandEnvNodes recursively expands ${VAR} references in YAML scalar node values.
// After expansion, the node tag is cleared so yaml.v3 re-infers the type from the
// expanded value (e.g., "5432" → int when the target field is int).
func expandEnvNodes(node *yaml.Node) {
	if node == nil {
		return
	}
	if node.Kind == yaml.ScalarNode {
		expanded := os.ExpandEnv(node.Value)
		if expanded != node.Value {
			node.Value = expanded
			// Clear the tag so yaml.v3 re-infers the type from the new value.
			// Without this, a value like ${PORT} that was tagged !!str would
			// remain a string even after expanding to "5432".
			node.Tag = ""
		}
		return
	}
	for _, child := range node.Content {
		expandEnvNodes(child)
	}
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
		}
	}
	return nil
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
