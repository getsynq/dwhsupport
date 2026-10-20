package bigquery

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

func TestFieldIsNullable(t *testing.T) {
	require.True(t, fieldIsNullable(&bigquery.FieldSchema{Name: "a"}))
	require.False(t, fieldIsNullable(&bigquery.FieldSchema{Name: "a", Required: true}))
	require.False(t, fieldIsNullable(&bigquery.FieldSchema{Name: "a", Repeated: true}))
}

func TestExtractConstraintsFromMeta_PrimaryKey(t *testing.T) {
	meta := &bigquery.TableMetadata{
		TableConstraints: &bigquery.TableConstraints{
			PrimaryKey: &bigquery.PrimaryKey{Columns: []string{"order_id", "item_id"}},
		},
		Clustering: &bigquery.Clustering{Fields: []string{"order_id"}},
	}

	rows := extractConstraintsFromMeta("project", "dataset", "order_items", meta)

	require.Equal(t, []*scrapper.TableConstraintRow{
		{
			Database: "project", Schema: "dataset", Table: "order_items", ConstraintName: "primary_key",
			ColumnName: "order_id", ConstraintType: scrapper.ConstraintTypePrimaryKey, ColumnPosition: 1, IsEnforced: lo.ToPtr(false),
		},
		{
			Database: "project", Schema: "dataset", Table: "order_items", ConstraintName: "primary_key",
			ColumnName: "item_id", ConstraintType: scrapper.ConstraintTypePrimaryKey, ColumnPosition: 2, IsEnforced: lo.ToPtr(false),
		},
		{
			Database: "project", Schema: "dataset", Table: "order_items", ConstraintName: "clustering",
			ColumnName: "order_id", ConstraintType: scrapper.ConstraintTypeClusterBy, ColumnPosition: 1,
		},
	}, rows)
}

func TestExtractConstraintsFromMeta_NoPrimaryKey(t *testing.T) {
	for name, meta := range map[string]*bigquery.TableMetadata{
		"no constraints":            {},
		"constraints without a key": {TableConstraints: &bigquery.TableConstraints{}},
	} {
		t.Run(name, func(t *testing.T) {
			require.Empty(t, extractConstraintsFromMeta("project", "dataset", "t", meta))
		})
	}
}
