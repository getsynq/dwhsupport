# dwhsupport

A Go library that provides standardized interfaces and models for interacting with different data warehouses (DWH). This library abstracts away the complexities of working with various data warehouse systems by providing a unified interface for common operations.

## Core Components

### Scrapper Interface

The `Scrapper` interface provides methods for:

- Validating configurations
- Querying catalog information
- Fetching table metrics
- Retrieving SQL definitions
- Managing database connections

### Data Models

The library includes comprehensive models for:

- `TableMetricsRow`: Table statistics and metadata
- `CatalogColumnRow`: Detailed column information
- `TableRow`: Table structure and properties
- `SqlDefinitionRow`: SQL definitions for views/tables
- `DatabaseRow`: Database-level metadata

### Scope Filtering

The `scrapper/scope` package provides include/exclude filtering at database, schema, and table levels with glob pattern support:

```go
// Define a scope filter
filter := &scope.ScopeFilter{
    Include: []scope.ScopeRule{
        {Database: "prod_*", Schema: "public"},
    },
    Exclude: []scope.ScopeRule{
        {Database: "prod_staging"},
    },
}

// Option 1: Wrap a scrapper with ScopedScrapper for automatic filtering
scoped := scope.NewScopedScrapper(inner, filter)
tables, err := scoped.QueryTables(ctx) // automatically filtered

// Option 2: Inject scope via context for SQL push-down
ctx = scope.WithScope(ctx, filter)
tables, err := inner.QueryTables(ctx) // filter conditions pushed into SQL
```

`ScopedScrapper` applies filtering at two levels:
- **SQL push-down** — scope conditions are injected directly into warehouse queries for efficiency
- **Post-filtering** — results are filtered in-memory to guarantee compliance

### Query Stats

The `exec/querystats` package provides query execution statistics via context:

```go
ctx = querystats.WithCallback(ctx, func(stats querystats.QueryStats) {
    log.Printf("Query %s: %d rows read, %d bytes, %v",
        stats.QueryID, stats.RowsRead, stats.BytesRead, stats.Duration)
})

// All queries executed with this context will report stats via the callback
tables, err := scrapper.QueryTables(ctx)
```

### Query Processing

The `QueryMany` type provides flexible options for:

- Batch processing of query results
- Post-processing of rows
- Custom argument handling
- Configurable batch sizes

## Usage

```go
scrapper := NewYourDwhScrapper(config)
defer scrapper.Close()

// Query table metrics
metrics, err := scrapper.QueryTableMetrics(ctx, lastFetchTime)

// Get catalog information
catalog, err := scrapper.QueryCatalog(ctx)

// Query SQL definitions
definitions, err := scrapper.QuerySqlDefinitions(ctx)

// Scoped queries — only return results matching the filter
filter := &scope.ScopeFilter{
    Include: []scope.ScopeRule{{Schema: "analytics"}},
}
scoped := scope.NewScopedScrapper(scrapper, filter)
tables, err := scoped.QueryTables(ctx)
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

Copyright 2024 SYNQ

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

## Support

For support, please open an issue in the GitHub repository.
