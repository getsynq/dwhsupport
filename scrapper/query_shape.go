package scrapper

type QueryShapeColumn struct {
	Name       string `json:"name"`
	NativeType string `json:"native_type"`
	Position   int32  `json:"position"`
	// Kind is what NativeType means on this warehouse, filled in by
	// RunRawQuery and QueryShape. Pass it to SqlLiteral to write a value of
	// this column back into SQL. KindUnknown when the type is not one
	// NativeValueKind recognises.
	Kind ValueKind `json:"kind,omitempty"`
}

// SetKinds fills each column's Kind from its NativeType for the given
// dialect (Scrapper.DialectType()) and returns cols.
func SetKinds(dialect string, cols []*QueryShapeColumn) []*QueryShapeColumn {
	for _, c := range cols {
		c.Kind = NativeValueKind(dialect, c.NativeType)
	}
	return cols
}
