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

### Query Processing

The `QueryMany` type provides flexible options for:

- Batch processing of query results
- Post-processing of rows
- Custom argument handling
- Configurable batch sizes

## Usage

```go
// Example of using the scrapper interface
scrapper := NewYourDwhScrapper(config)
defer scrapper.Close()

// Query table metrics
metrics, err := scrapper.QueryTableMetrics(ctx, lastFetchTime)

// Get catalog information
catalog, err := scrapper.QueryCatalog(ctx)

// Query SQL definitions
definitions, err := scrapper.QuerySqlDefinitions(ctx)
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
