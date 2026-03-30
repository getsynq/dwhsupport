// seed creates the standard test datasets (synq_test_a, synq_test_b) in BigQuery.
// Run once to set up test data for integration tests:
//
//	go run ./scrapper/bigquery/testdata/seed/
//
// Requires BIGQUERY_PROJECT_ID and BIGQUERY_CREDENTIALS_FILE env vars (loaded from .env).
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"cloud.google.com/go/bigquery"
	"github.com/joho/godotenv"
	"google.golang.org/api/option"
)

func main() {
	_ = godotenv.Load(".env")

	projectID := os.Getenv("BIGQUERY_PROJECT_ID")
	if projectID == "" {
		log.Fatal("BIGQUERY_PROJECT_ID not set")
	}

	credsFile := os.Getenv("BIGQUERY_CREDENTIALS_FILE")
	if credsFile == "" {
		log.Fatal("BIGQUERY_CREDENTIALS_FILE not set")
	}
	credsJSON, err := os.ReadFile(credsFile)
	if err != nil {
		log.Fatalf("reading credentials: %v", err)
	}

	ctx := context.Background()
	client, err := bigquery.NewClient(ctx, projectID,
		option.WithCredentialsJSON(credsJSON),
		option.WithUserAgent("synq-bq-seed-v1.0.0"),
	)
	if err != nil {
		log.Fatalf("creating client: %v", err)
	}
	defer client.Close()

	region := os.Getenv("BIGQUERY_REGION")
	if region == "" {
		region = "EU"
	}

	seedDatasetA(ctx, client, projectID, region)
	seedDatasetB(ctx, client, projectID, region)

	log.Println("Seeding complete!")
}

func execSQL(ctx context.Context, client *bigquery.Client, sql string) {
	q := client.Query(sql)
	job, err := q.Run(ctx)
	if err != nil {
		log.Fatalf("running query: %v\nSQL: %s", err, sql)
	}
	_, err = job.Read(ctx)
	if err != nil {
		log.Fatalf("reading result: %v\nSQL: %s", err, sql)
	}
}

func createDataset(ctx context.Context, client *bigquery.Client, name, region string) {
	ds := client.Dataset(name)
	meta, err := ds.Metadata(ctx)
	if err == nil {
		log.Printf("Dataset %s already exists (location=%s)", name, meta.Location)
		return
	}
	err = ds.Create(ctx, &bigquery.DatasetMetadata{
		Location:    region,
		Description: fmt.Sprintf("DWH testing dataset: %s", name),
	})
	if err != nil {
		log.Fatalf("creating dataset %s: %v", name, err)
	}
	log.Printf("Created dataset %s in %s", name, region)
}

func seedDatasetA(ctx context.Context, client *bigquery.Client, projectID, region string) {
	const ds = "synq_test_a"
	createDataset(ctx, client, ds, region)

	fqn := func(table string) string {
		return fmt.Sprintf("`%s.%s.%s`", projectID, ds, table)
	}

	log.Println("Creating synq_test_a.products...")
	execSQL(ctx, client, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT64 NOT NULL,
			name STRING NOT NULL,
			description STRING,
			price NUMERIC(10,2) NOT NULL,
			quantity INT64 NOT NULL,
			weight FLOAT64,
			is_active BOOL NOT NULL,
			created_at TIMESTAMP NOT NULL,
			updated_at TIMESTAMP,
			category STRING,
			sku STRING
		)
		OPTIONS (description = 'Product catalog with inventory tracking')
	`, fqn("products")))

	// Truncate and re-insert for idempotency
	execSQL(ctx, client, fmt.Sprintf(`DELETE FROM %s WHERE true`, fqn("products")))
	execSQL(ctx, client, fmt.Sprintf(`
		INSERT INTO %s (id, name, description, price, quantity, weight, is_active, created_at, category, sku)
		VALUES
			(1, 'Widget A', 'A standard widget', 19.99, 100, 0.5, true, TIMESTAMP('2024-01-15 10:00:00 UTC'), 'Electronics', 'WIDG-A-00001'),
			(2, 'Widget B', 'A premium widget', 49.99, 50, 1.2, true, TIMESTAMP('2024-02-20 14:30:00 UTC'), 'Electronics', 'WIDG-B-00002'),
			(3, 'Gadget X', 'Discontinued gadget', 9.99, 0, 0.3, false, TIMESTAMP('2024-03-10 09:15:00 UTC'), 'Accessories', 'GADG-X-00003')
	`, fqn("products")))

	log.Println("Creating synq_test_a.order_items...")
	execSQL(ctx, client, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			order_id INT64 NOT NULL,
			item_id INT64 NOT NULL,
			product_id INT64 NOT NULL,
			quantity INT64 NOT NULL,
			unit_price NUMERIC(10,2) NOT NULL,
			discount NUMERIC(5,2)
		)
		OPTIONS (description = 'Line items within customer orders')
	`, fqn("order_items")))

	execSQL(ctx, client, fmt.Sprintf(`DELETE FROM %s WHERE true`, fqn("order_items")))
	execSQL(ctx, client, fmt.Sprintf(`
		INSERT INTO %s (order_id, item_id, product_id, quantity, unit_price, discount)
		VALUES
			(1, 1, 1, 2, 19.99, 0),
			(1, 2, 2, 1, 49.99, 5.00),
			(2, 1, 3, 5, 9.99, 0)
	`, fqn("order_items")))

	log.Println("Creating synq_test_a.active_products view...")
	execSQL(ctx, client, fmt.Sprintf(`
		CREATE OR REPLACE VIEW %s AS
		SELECT id, name, price, quantity, category
		FROM %s
		WHERE is_active = true
	`, fqn("active_products"), fqn("products")))

	log.Println("Creating synq_test_a.order_summary view...")
	execSQL(ctx, client, fmt.Sprintf(`
		CREATE OR REPLACE VIEW %s AS
		SELECT
			order_id,
			COUNT(*) AS item_count,
			SUM(quantity * CAST(unit_price AS FLOAT64) - CAST(discount AS FLOAT64)) AS total_amount
		FROM %s
		GROUP BY order_id
	`, fqn("order_summary"), fqn("order_items")))

	// Partitioned + clustered table for constraint tests
	log.Println("Creating synq_test_a.partitioned_products...")
	execSQL(ctx, client, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT64 NOT NULL,
			name STRING NOT NULL,
			price NUMERIC(10,2) NOT NULL,
			category STRING,
			created_at TIMESTAMP NOT NULL
		)
		PARTITION BY DATE(created_at)
		CLUSTER BY category, name
		OPTIONS (description = 'Partitioned and clustered product table for constraint tests')
	`, fqn("partitioned_products")))

	execSQL(ctx, client, fmt.Sprintf(`DELETE FROM %s WHERE true`, fqn("partitioned_products")))
	execSQL(ctx, client, fmt.Sprintf(`
		INSERT INTO %s (id, name, price, category, created_at)
		VALUES
			(1, 'Widget A', 19.99, 'Electronics', TIMESTAMP('2024-01-15 10:00:00 UTC')),
			(2, 'Widget B', 49.99, 'Electronics', TIMESTAMP('2024-02-20 14:30:00 UTC')),
			(3, 'Gadget X', 9.99, 'Accessories', TIMESTAMP('2024-03-10 09:15:00 UTC'))
	`, fqn("partitioned_products")))

	// Advanced type test table (NUMERIC, BIGNUMERIC, TIMESTAMP, BOOL)
	// Matches the pattern from ClickHouse's test_clickhouse_scrapper in dwhtesting
	log.Println("Creating synq_test_a.test_bigquery_types...")
	execSQL(ctx, client, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT64,
			name STRING,
			amount NUMERIC,
			big_amount BIGNUMERIC,
			created_at TIMESTAMP,
			is_active BOOL
		)
		OPTIONS (description = 'Test table for advanced BigQuery types')
	`, fqn("test_bigquery_types")))

	execSQL(ctx, client, fmt.Sprintf(`DELETE FROM %s WHERE true`, fqn("test_bigquery_types")))
	execSQL(ctx, client, fmt.Sprintf(`
		INSERT INTO %s (id, name, amount, big_amount, created_at, is_active)
		VALUES
			(1, 'Alice', 100.50, 12345678901234567890.123456789012345678, TIMESTAMP('2024-01-01 10:00:00'), true),
			(2, 'Bob', 200.75, 99999999999999999999999999999999999999.999999999999999999, TIMESTAMP('2024-01-02 11:00:00'), false)
	`, fqn("test_bigquery_types")))

	log.Println("Creating synq_test_a.test_bigquery_types_view...")
	execSQL(ctx, client, fmt.Sprintf(`
		CREATE OR REPLACE VIEW %s AS
		SELECT id, name, amount FROM %s WHERE is_active = true
	`, fqn("test_bigquery_types_view"), fqn("test_bigquery_types")))
}

func seedDatasetB(ctx context.Context, client *bigquery.Client, projectID, region string) {
	const ds = "synq_test_b"
	createDataset(ctx, client, ds, region)

	fqn := func(table string) string {
		return fmt.Sprintf("`%s.%s.%s`", projectID, ds, table)
	}

	log.Println("Creating synq_test_b.customers...")
	execSQL(ctx, client, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT64 NOT NULL,
			email STRING NOT NULL,
			full_name STRING,
			region STRING
		)
		OPTIONS (description = 'Customer directory')
	`, fqn("customers")))

	execSQL(ctx, client, fmt.Sprintf(`DELETE FROM %s WHERE true`, fqn("customers")))
	execSQL(ctx, client, fmt.Sprintf(`
		INSERT INTO %s (id, email, full_name, region)
		VALUES
			(1, 'alice@example.com', 'Alice Smith', 'US'),
			(2, 'bob@example.com', 'Bob Jones', 'EU')
	`, fqn("customers")))

	log.Println("Creating synq_test_b.customer_regions view...")
	execSQL(ctx, client, fmt.Sprintf(`
		CREATE OR REPLACE VIEW %s AS
		SELECT DISTINCT region FROM %s WHERE region IS NOT NULL
	`, fqn("customer_regions"), fqn("customers")))
}
