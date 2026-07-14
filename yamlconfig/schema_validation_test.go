package yamlconfig

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/xeipuuv/gojsonschema"
	"gopkg.in/yaml.v3"
)

// connectionsUnderSchema builds the JSON document ({"connections": {...}}) that
// the ConnectionsSchema wrapper describes, from raw YAML. It decodes into a
// generic map so the JSON carries the YAML field names the schema is keyed on
// (the reflector uses the "yaml" tag), then round-trips through JSON to
// normalize scalar types for the validator.
func connectionsUnderSchema(t *testing.T, rawYAML []byte) []byte {
	t.Helper()
	var doc map[string]any
	require.NoError(t, yaml.Unmarshal(rawYAML, &doc))
	require.Contains(t, doc, "connections", "fixture has no connections block")
	out, err := json.Marshal(map[string]any{"connections": doc["connections"]})
	require.NoError(t, err)
	return out
}

func validateAgainstConnectionsSchema(t *testing.T, docJSON []byte) *gojsonschema.Result {
	t.Helper()
	schemaJSON, err := json.Marshal(ConnectionsSchema())
	require.NoError(t, err)
	res, err := gojsonschema.Validate(
		gojsonschema.NewBytesLoader(schemaJSON),
		gojsonschema.NewBytesLoader(docJSON),
	)
	require.NoError(t, err)
	return res
}

// TestFullExampleMatchesSchema guards against drift between the generated JSON
// schema (ConnectionsSchema) and the documented full_example.yaml fixture: every
// connection in the fixture must validate against the schema, including required
// fields, additionalProperties:false, and nested $ref types (scope). Parsed
// without env expansion, so all scalar types stay as authored (secrets remain
// literal "${VAR}" strings, which are valid for the string-typed fields).
func TestFullExampleMatchesSchema(t *testing.T) {
	raw, err := os.ReadFile("testdata/full_example.yaml")
	require.NoError(t, err)

	res := validateAgainstConnectionsSchema(t, connectionsUnderSchema(t, raw))
	if !res.Valid() {
		for _, e := range res.Errors() {
			t.Errorf("full_example.yaml does not match generated schema: %s", e)
		}
	}
}

// TestSchemaRejectsInvalidConnection confirms the schema check above is
// meaningful — the validator must catch a missing required field, an unknown
// property, and an unknown property inside the nested scope $ref. If this ever
// passes, ConnectionsSchema has silently become too permissive and
// TestFullExampleMatchesSchema can no longer be trusted.
func TestSchemaRejectsInvalidConnection(t *testing.T) {
	bad := `{"connections":{"x":{"fabric":{"database":"d","bogus_field":1,"scope":{"include":[{"nope":"y"}]}}}}}`
	res := validateAgainstConnectionsSchema(t, []byte(bad))
	require.False(t, res.Valid(), "expected invalid connection to be rejected by the schema")

	var msgs string
	for _, e := range res.Errors() {
		msgs += e.String() + "\n"
	}
	require.Contains(t, msgs, "host is required")
	require.Contains(t, msgs, "bogus_field")
	require.Contains(t, msgs, "nope") // nested $ref (ScopeConf → ScopeRuleConf) is resolved
}
