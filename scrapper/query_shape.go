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
	// Precision and Scale are an exact numeric column's declared size
	// (KindNumeric), from the driver (sql.ColumnType.DecimalSize) or, when it
	// reports none, from NativeType (DECIMAL(20,6)). Both are nil for every
	// other kind, and for a numeric whose size is not known: one declared
	// without it (a Postgres NUMERIC, an Oracle NUMBER), or one whose driver
	// reports neither and names it bare (Databricks, Athena). A nil scale is
	// not a scale of 0.
	Precision *int64 `json:"precision,omitempty"`
	Scale     *int64 `json:"scale,omitempty"`
}

// SetKinds fills each column's Kind from its NativeType for the given
// dialect (Scrapper.DialectType()) and returns cols.
func SetKinds(dialect string, cols []*QueryShapeColumn) []*QueryShapeColumn {
	for _, c := range cols {
		c.Kind = NativeValueKind(dialect, c.NativeType)
	}
	return cols
}
