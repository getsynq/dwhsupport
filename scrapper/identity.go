package scrapper

// HasValidIdentity reports whether all identity fields of the row — those that
// form the object's fully-qualified name — are free of NUL bytes and valid UTF-8.
// A row with an invalid identity cannot be safely addressed downstream and should
// be dropped rather than sanitised (a sanitised name might collide with another
// object or refer to nothing that exists in the warehouse).

func (r *TableMetricsRow) HasValidIdentity() bool {
	if r == nil {
		return false
	}
	return IsValidString(r.Instance) && IsValidString(r.Database) && IsValidString(r.Schema) && IsValidString(r.Table)
}

func (r *CatalogColumnRow) HasValidIdentity() bool {
	if r == nil {
		return false
	}
	return IsValidString(r.Instance) &&
		IsValidString(r.Database) &&
		IsValidString(r.Schema) &&
		IsValidString(r.Table) &&
		IsValidString(r.Column)
}

func (r *TableRow) HasValidIdentity() bool {
	if r == nil {
		return false
	}
	return IsValidString(r.Instance) && IsValidString(r.Database) && IsValidString(r.Schema) && IsValidString(r.Table)
}

func (r *SqlDefinitionRow) HasValidIdentity() bool {
	if r == nil {
		return false
	}
	return IsValidString(r.Instance) && IsValidString(r.Database) && IsValidString(r.Schema) && IsValidString(r.Table)
}

func (r *DatabaseRow) HasValidIdentity() bool {
	if r == nil {
		return false
	}
	return IsValidString(r.Instance) && IsValidString(r.Database)
}

func (r *TableConstraintRow) HasValidIdentity() bool {
	if r == nil {
		return false
	}
	return IsValidString(r.Instance) &&
		IsValidString(r.Database) &&
		IsValidString(r.Schema) &&
		IsValidString(r.Table) &&
		IsValidString(r.ConstraintName) &&
		IsValidString(r.ColumnName)
}

func (r *SegmentRow) HasValidIdentity() bool {
	if r == nil {
		return false
	}
	return IsValidString(r.Segment)
}

func (c *QueryShapeColumn) HasValidIdentity() bool {
	if c == nil {
		return false
	}
	return IsValidString(c.Name)
}

func (v *SegmentValue) HasValidIdentity() bool {
	if v == nil {
		return false
	}
	return IsValidString(v.Name)
}

func (v *ColumnValue) HasValidIdentity() bool {
	if v == nil {
		return false
	}
	return IsValidString(v.Name)
}

// HasValidIdentity on CustomMetricsRow requires every segment and column name to be valid;
// a single bad identifier changes the meaning of the whole row, so we drop rather than partially filter.
func (r *CustomMetricsRow) HasValidIdentity() bool {
	if r == nil {
		return false
	}
	for _, s := range r.Segments {
		if !s.HasValidIdentity() {
			return false
		}
	}
	for _, c := range r.ColumnValues {
		if !c.HasValidIdentity() {
			return false
		}
	}
	return true
}

func (t *Tag) HasValidIdentity() bool {
	if t == nil {
		return false
	}
	return IsValidString(t.TagName) && IsValidString(t.TagValue)
}

func (a *Annotation) HasValidIdentity() bool {
	if a == nil {
		return false
	}
	return IsValidString(a.AnnotationName) && IsValidString(a.AnnotationValue)
}

func (f *SchemaColumnField) HasValidIdentity() bool {
	if f == nil {
		return false
	}
	if !IsValidString(f.Name) || !IsValidString(f.HumanName) || !IsValidString(f.NativeType) {
		return false
	}
	for _, child := range f.Fields {
		if !child.HasValidIdentity() {
			return false
		}
	}
	return true
}
