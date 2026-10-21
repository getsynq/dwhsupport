package bigquery

import (
	"testing"

	"cloud.google.com/go/bigquery"
	"github.com/getsynq/dwhsupport/scrapper"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
)

func TestExtractConstraintsFromMeta_ForeignKeys(t *testing.T) {
	meta := &bigquery.TableMetadata{
		TableConstraints: &bigquery.TableConstraints{
			ForeignKeys: []*bigquery.ForeignKey{
				{
					Name:            "fk_product",
					ReferencedTable: &bigquery.Table{ProjectID: "project", DatasetID: "dataset", TableID: "products"},
					ColumnReferences: []*bigquery.ColumnReference{
						{ReferencingColumn: "product_id", ReferencedColumn: "id"},
						{ReferencingColumn: "variant", ReferencedColumn: "variant"},
					},
				},
				{
					// Unnamed, and in another project.
					ReferencedTable:  &bigquery.Table{ProjectID: "other", DatasetID: "crm", TableID: "customers"},
					ColumnReferences: []*bigquery.ColumnReference{{ReferencingColumn: "customer_id", ReferencedColumn: "id"}},
				},
				{
					// Unnamed and with no project: the table's own.
					ReferencedTable:  &bigquery.Table{DatasetID: "dataset", TableID: "orders"},
					ColumnReferences: []*bigquery.ColumnReference{{ReferencingColumn: "order_id", ReferencedColumn: "id"}},
				},
				{Name: "no_table", ColumnReferences: []*bigquery.ColumnReference{{ReferencingColumn: "x", ReferencedColumn: "y"}}},
				nil,
			},
		},
	}

	rows := extractConstraintsFromMeta("project", "dataset", "order_items", meta)

	fk := func(name, column string, position int32, project, dataset, table, referencedColumn string) *scrapper.TableConstraintRow {
		return &scrapper.TableConstraintRow{
			Database: "project", Schema: "dataset", Table: "order_items", ConstraintName: name,
			ColumnName: column, ConstraintType: scrapper.ConstraintTypeForeignKey, ColumnPosition: position, IsEnforced: lo.ToPtr(false),
			ReferencedDatabase: project, ReferencedSchema: dataset, ReferencedTable: table, ReferencedColumnName: referencedColumn,
		}
	}
	require.Equal(t, []*scrapper.TableConstraintRow{
		fk("fk_product", "product_id", 1, "project", "dataset", "products", "id"),
		fk("fk_product", "variant", 2, "project", "dataset", "products", "variant"),
		fk("foreign_key_2", "customer_id", 1, "other", "crm", "customers", "id"),
		fk("foreign_key_3", "order_id", 1, "project", "dataset", "orders", "id"),
	}, rows)
}

func TestExtractConstraintsFromMeta_IngestionTimePartitioning(t *testing.T) {
	for _, tc := range []struct {
		partitioning *bigquery.TimePartitioning
		column       string
	}{
		// PARTITION BY _PARTITIONDATE, or DATE(_PARTITIONTIME)
		{&bigquery.TimePartitioning{Type: bigquery.DayPartitioningType}, "_PARTITIONDATE"},
		// PARTITION BY TIMESTAMP_TRUNC(_PARTITIONTIME, HOUR), which has no _PARTITIONDATE
		{&bigquery.TimePartitioning{Type: bigquery.HourPartitioningType}, "_PARTITIONTIME"},
		{&bigquery.TimePartitioning{Type: bigquery.MonthPartitioningType}, "_PARTITIONTIME"},
		// A column-partitioned table names its column.
		{&bigquery.TimePartitioning{Type: bigquery.DayPartitioningType, Field: "created_at"}, "created_at"},
	} {
		rows := extractConstraintsFromMeta("project", "dataset", "page_views", &bigquery.TableMetadata{TimePartitioning: tc.partitioning})
		require.Len(t, rows, 1, "%+v", tc.partitioning)
		require.Equal(t, &scrapper.TableConstraintRow{
			Database:       "project",
			Schema:         "dataset",
			Table:          "page_views",
			ConstraintName: "time_partitioning_" + string(tc.partitioning.Type),
			ColumnName:     tc.column,
			ConstraintType: scrapper.ConstraintTypePartitionBy,
			ColumnPosition: 1,
		}, rows[0])
	}
}
