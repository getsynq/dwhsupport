package scrapper

import (
	"strings"
	"unicode/utf8"
)

// IsValidString reports whether s contains no NUL bytes and is valid UTF-8.
// Used both as the fast-path check in SanitizeString and as the identity-field
// validity predicate by the rejecting scrapper decorator.
func IsValidString(s string) bool {
	return strings.IndexByte(s, 0) == -1 && utf8.ValidString(s)
}

// SanitizeString drops NUL bytes and invalid UTF-8 sequences.
// Returns the input unchanged (no allocation) when it contains neither — the common case.
// Dropping rather than substituting (e.g. with U+FFFD) avoids visible replacement-character
// artifacts in downstream UIs that would otherwise surface data corruption to end users.
func SanitizeString(s string) string {
	if IsValidString(s) {
		return s
	}
	return strings.ReplaceAll(strings.ToValidUTF8(s, ""), "\x00", "")
}

// SanitizeStringPtr applies SanitizeString through a *string, leaving nil pointers untouched.
func SanitizeStringPtr(s *string) {
	if s == nil {
		return
	}
	*s = SanitizeString(*s)
}

func (r *TableMetricsRow) Sanitize() {
	if r == nil {
		return
	}
	r.Instance = SanitizeString(r.Instance)
	r.Database = SanitizeString(r.Database)
	r.Schema = SanitizeString(r.Schema)
	r.Table = SanitizeString(r.Table)
}

func (t *Tag) Sanitize() {
	if t == nil {
		return
	}
	t.TagName = SanitizeString(t.TagName)
	t.TagValue = SanitizeString(t.TagValue)
}

func (a *Annotation) Sanitize() {
	if a == nil {
		return
	}
	a.AnnotationName = SanitizeString(a.AnnotationName)
	a.AnnotationValue = SanitizeString(a.AnnotationValue)
}

func (f *SchemaColumnField) Sanitize() {
	if f == nil {
		return
	}
	f.Name = SanitizeString(f.Name)
	f.HumanName = SanitizeString(f.HumanName)
	f.NativeType = SanitizeString(f.NativeType)
	SanitizeStringPtr(f.Description)
	for _, child := range f.Fields {
		child.Sanitize()
	}
}

func (r *CatalogColumnRow) Sanitize() {
	if r == nil {
		return
	}
	r.Instance = SanitizeString(r.Instance)
	r.Database = SanitizeString(r.Database)
	r.Schema = SanitizeString(r.Schema)
	r.Table = SanitizeString(r.Table)
	r.TableType = SanitizeString(r.TableType)
	r.Column = SanitizeString(r.Column)
	r.Type = SanitizeString(r.Type)
	SanitizeStringPtr(r.Comment)
	SanitizeStringPtr(r.TableComment)
	for _, t := range r.ColumnTags {
		t.Sanitize()
	}
	for _, t := range r.TableTags {
		t.Sanitize()
	}
	for _, f := range r.FieldSchemas {
		f.Sanitize()
	}
}

func (r *TableRow) Sanitize() {
	if r == nil {
		return
	}
	r.Instance = SanitizeString(r.Instance)
	r.Database = SanitizeString(r.Database)
	r.Schema = SanitizeString(r.Schema)
	r.Table = SanitizeString(r.Table)
	r.TableType = SanitizeString(r.TableType)
	SanitizeStringPtr(r.Description)
	for _, t := range r.Tags {
		t.Sanitize()
	}
	for _, a := range r.Annotations {
		a.Sanitize()
	}
	for _, c := range r.Constraints {
		c.Sanitize()
	}
	// Options holds warehouse-specific metadata (map[string]interface{}); its string
	// values originate from typed warehouse APIs (bigquery.TableMetadata, etc.) rather
	// than raw column data, so they are not sanitised here.
}

func (r *SqlDefinitionRow) Sanitize() {
	if r == nil {
		return
	}
	r.Instance = SanitizeString(r.Instance)
	r.Database = SanitizeString(r.Database)
	r.Schema = SanitizeString(r.Schema)
	r.Table = SanitizeString(r.Table)
	r.TableType = SanitizeString(r.TableType)
	r.Sql = SanitizeString(r.Sql)
	SanitizeStringPtr(r.Description)
	for _, t := range r.Tags {
		t.Sanitize()
	}
}

func (r *DatabaseRow) Sanitize() {
	if r == nil {
		return
	}
	r.Instance = SanitizeString(r.Instance)
	r.Database = SanitizeString(r.Database)
	SanitizeStringPtr(r.Description)
	SanitizeStringPtr(r.DatabaseType)
	SanitizeStringPtr(r.DatabaseOwner)
}

func (r *TableConstraintRow) Sanitize() {
	if r == nil {
		return
	}
	r.Instance = SanitizeString(r.Instance)
	r.Database = SanitizeString(r.Database)
	r.Schema = SanitizeString(r.Schema)
	r.Table = SanitizeString(r.Table)
	r.ConstraintName = SanitizeString(r.ConstraintName)
	r.ColumnName = SanitizeString(r.ColumnName)
	r.ConstraintType = SanitizeString(r.ConstraintType)
	r.ConstraintExpression = SanitizeString(r.ConstraintExpression)
}

func (r *SegmentRow) Sanitize() {
	if r == nil {
		return
	}
	r.Segment = SanitizeString(r.Segment)
}

func (c *QueryShapeColumn) Sanitize() {
	if c == nil {
		return
	}
	c.Name = SanitizeString(c.Name)
	c.NativeType = SanitizeString(c.NativeType)
}

func (v *SegmentValue) Sanitize() {
	if v == nil {
		return
	}
	v.Name = SanitizeString(v.Name)
	v.Value = SanitizeString(v.Value)
}

func (v *ColumnValue) Sanitize() {
	if v == nil {
		return
	}
	v.Name = SanitizeString(v.Name)
	if s, ok := v.Value.(StringValue); ok {
		v.Value = StringValue(SanitizeString(string(s)))
	}
}

func (r *CustomMetricsRow) Sanitize() {
	if r == nil {
		return
	}
	for _, s := range r.Segments {
		s.Sanitize()
	}
	for _, c := range r.ColumnValues {
		c.Sanitize()
	}
}
