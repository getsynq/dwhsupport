package yamlconfig

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/xeipuuv/gojsonschema"
	"gopkg.in/yaml.v3"
)

func TestScratchValidateFixture(t *testing.T) {
	schemaJSON, _ := json.Marshal(ConnectionsSchema())
	raw, _ := os.ReadFile("testdata/full_example.yaml")
	var doc map[string]interface{}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	docJSON, _ := json.Marshal(map[string]interface{}{"connections": doc["connections"]})
	res, err := gojsonschema.Validate(gojsonschema.NewBytesLoader(schemaJSON), gojsonschema.NewBytesLoader(docJSON))
	if err != nil {
		t.Fatalf("VALIDATOR ERROR: %v", err)
	}
	if res.Valid() {
		t.Logf("FIXTURE VALID against generated schema")
	} else {
		for _, e := range res.Errors() {
			t.Errorf("SCHEMA MISMATCH: %s", e)
		}
	}
}
