package scrapper

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsValidString(t *testing.T) {
	assert.True(t, IsValidString(""))
	assert.True(t, IsValidString("hello"))
	assert.True(t, IsValidString("hello — π"))
	assert.False(t, IsValidString("a\x00b"), "NUL is valid UTF-8 but still rejected")
	assert.False(t, IsValidString("a\xc3b"), "lone continuation byte")
}

func TestCatalogColumnRow_HasValidIdentity(t *testing.T) {
	good := &CatalogColumnRow{Instance: "i", Database: "d", Schema: "s", Table: "t", Column: "c"}
	assert.True(t, good.HasValidIdentity())

	bad := &CatalogColumnRow{Instance: "i", Database: "d\x00", Schema: "s", Table: "t", Column: "c"}
	assert.False(t, bad.HasValidIdentity())

	var nilRow *CatalogColumnRow
	assert.False(t, nilRow.HasValidIdentity())
}

func TestCustomMetricsRow_HasValidIdentity_BadSegment(t *testing.T) {
	row := &CustomMetricsRow{
		Segments: []*SegmentValue{
			{Name: "ok", Value: "v"},
			{Name: "bad\x00", Value: "v"},
		},
	}
	assert.False(t, row.HasValidIdentity())
}

func TestSchemaColumnField_HasValidIdentity_Recursive(t *testing.T) {
	good := &SchemaColumnField{
		Name: "outer",
		Fields: []*SchemaColumnField{
			{Name: "inner"},
		},
	}
	assert.True(t, good.HasValidIdentity())

	bad := &SchemaColumnField{
		Name: "outer",
		Fields: []*SchemaColumnField{
			{Name: "inner\x00"},
		},
	}
	assert.False(t, bad.HasValidIdentity())
}
