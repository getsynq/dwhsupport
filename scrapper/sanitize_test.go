package scrapper

import (
	"strings"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSanitizeString_CleanPassthrough(t *testing.T) {
	clean := "hello world — π"
	out := SanitizeString(clean)
	assert.Equal(t, clean, out)

	// The clean path must share the input's backing array — proof we skipped the copy.
	assert.Same(t, unsafe.StringData(clean), unsafe.StringData(out))
}

func TestSanitizeString_StripsNullBytes(t *testing.T) {
	assert.Equal(t, "abc", SanitizeString("a\x00b\x00c"))
	assert.Equal(t, "", SanitizeString("\x00\x00"))
	assert.Equal(t, "trailing", SanitizeString("trailing\x00"))
}

func TestSanitizeString_DropsInvalidUTF8(t *testing.T) {
	// Lone 0xC3 continuation byte without its follow-up — invalid UTF-8.
	assert.Equal(t, "prepost", SanitizeString("pre\xc3post"))
}

func TestSanitizeString_HandlesBoth(t *testing.T) {
	assert.Equal(t, "abc", SanitizeString("a\x00b\xc3c\x00"))
}

func TestSanitizeString_NoAllocOnCleanPath(t *testing.T) {
	s := strings.Repeat("hello world ", 1000)
	allocs := testing.AllocsPerRun(100, func() {
		_ = SanitizeString(s)
	})
	assert.Zero(t, allocs, "clean path must not allocate")
}

func TestSanitizeStringPtr_NilSafe(t *testing.T) {
	SanitizeStringPtr(nil)
	s := "a\x00b"
	SanitizeStringPtr(&s)
	assert.Equal(t, "ab", s)
}

func TestCatalogColumnRow_Sanitize_Recursive(t *testing.T) {
	comment := "col\x00comment"
	row := &CatalogColumnRow{
		Database:     "dirty\x00value",
		Schema:       "ok",
		Table:        "users\x00",
		Column:       "id",
		Type:         "INT",
		Comment:      &comment,
		TableComment: nil,
		ColumnTags:   []*Tag{{TagName: "a\x00b", TagValue: "v"}},
		TableTags:    []*Tag{nil, {TagName: "ok", TagValue: "also\x00bad"}},
		FieldSchemas: []*SchemaColumnField{
			{
				Name:      "outer\x00",
				HumanName: "Outer",
				Fields: []*SchemaColumnField{
					{Name: "inner\x00", NativeType: "STRING\x00"},
				},
			},
		},
	}
	row.Sanitize()

	assert.Equal(t, "dirtyvalue", row.Database)
	assert.Equal(t, "users", row.Table)
	require.NotNil(t, row.Comment)
	assert.Equal(t, "colcomment", *row.Comment)
	assert.Equal(t, "ab", row.ColumnTags[0].TagName)
	assert.Nil(t, row.TableTags[0])
	assert.Equal(t, "alsobad", row.TableTags[1].TagValue)
	assert.Equal(t, "outer", row.FieldSchemas[0].Name)
	assert.Equal(t, "inner", row.FieldSchemas[0].Fields[0].Name)
	assert.Equal(t, "STRING", row.FieldSchemas[0].Fields[0].NativeType)
}

func TestTableRow_Sanitize(t *testing.T) {
	desc := "hello\x00world"
	row := &TableRow{
		Database:    "db\x00",
		Table:       "t",
		Description: &desc,
		Tags:        []*Tag{{TagName: "k\x00", TagValue: "v"}},
		Annotations: []*Annotation{{AnnotationName: "a\x00", AnnotationValue: "b"}},
		Constraints: []*TableConstraintRow{{ConstraintName: "pk\x00", ColumnName: "id"}},
	}
	row.Sanitize()

	assert.Equal(t, "db", row.Database)
	assert.Equal(t, "helloworld", *row.Description)
	assert.Equal(t, "k", row.Tags[0].TagName)
	assert.Equal(t, "a", row.Annotations[0].AnnotationName)
	assert.Equal(t, "pk", row.Constraints[0].ConstraintName)
}

func TestDatabaseRow_Sanitize(t *testing.T) {
	desc := "d\x00e"
	owner := "ow\x00ner"
	row := &DatabaseRow{
		Database:      "db\x00",
		Description:   &desc,
		DatabaseOwner: &owner,
	}
	row.Sanitize()
	assert.Equal(t, "db", row.Database)
	assert.Equal(t, "de", *row.Description)
	assert.Equal(t, "owner", *row.DatabaseOwner)
	assert.Nil(t, row.DatabaseType)
}

func TestCustomMetricsRow_Sanitize(t *testing.T) {
	row := &CustomMetricsRow{
		Segments: []*SegmentValue{
			{Name: "seg\x00", Value: "v\x00al"},
			nil,
		},
		ColumnValues: []*ColumnValue{
			{Name: "col\x00", Value: IntValue(1)},
		},
	}
	row.Sanitize()
	assert.Equal(t, "seg", row.Segments[0].Name)
	assert.Equal(t, "val", row.Segments[0].Value)
	assert.Equal(t, "col", row.ColumnValues[0].Name)
	assert.Equal(t, IntValue(1), row.ColumnValues[0].Value)
}

func TestSanitize_NilReceiverSafe(t *testing.T) {
	var (
		cat    *CatalogColumnRow
		table  *TableRow
		sqldef *SqlDefinitionRow
		db     *DatabaseRow
		cons   *TableConstraintRow
		seg    *SegmentRow
		qs     *QueryShapeColumn
		cm     *CustomMetricsRow
		tag    *Tag
		ann    *Annotation
		fs     *SchemaColumnField
		sv     *SegmentValue
		cv     *ColumnValue
		tm     *TableMetricsRow
	)
	cat.Sanitize()
	table.Sanitize()
	sqldef.Sanitize()
	db.Sanitize()
	cons.Sanitize()
	seg.Sanitize()
	qs.Sanitize()
	cm.Sanitize()
	tag.Sanitize()
	ann.Sanitize()
	fs.Sanitize()
	sv.Sanitize()
	cv.Sanitize()
	tm.Sanitize()
}
