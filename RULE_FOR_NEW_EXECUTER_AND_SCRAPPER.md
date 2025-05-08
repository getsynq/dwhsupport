# Rule for Adding a New Executer and Scrapper

## Executer

- Define a config struct for your DWH (e.g., `FoobarConf`).
- Create a directory for your DWH under exec/ and scrapper/ if not present.
- Implement an `Executor` interface (usually embeds or implements `stdsql.StdSqlExecutor` or similar).
- Implement a struct (e.g., `FoobarExecutor`) with connection logic, `GetDb()`, `QueryRows()`, `Exec()`, `Close()`.
- Provide a constructor (e.g., `NewFoobarExecutor(ctx, conf)`).
- Implement `NewQuerier[T any](conn *FoobarExecutor) querier.Querier[T]` returning a `stdsql.NewQuerier[T](conn.db)` or similar.
- Implement `query.go` with `NewQuerier[T any](conn *FoobarExecutor) querier.Querier[T]` returning a `stdsql.NewQuerier[T](conn.db)` or similar.

## Scrapper

- Define a config struct for your scrapper, often reusing the exec config.
- Implement the `Scrapper` interface from `scrapper/interface.go`. If not all methods are implemented, stub them to return `ErrUnsupported`.
- Implement a struct (e.g., `FoobarScrapper`) with fields for config and executor.
- Provide a constructor (e.g., `NewFoobarScrapper(ctx, conf)`), which instantiates the executor.
- Implement all required methods (`DialectType`, `SqlDialect`, `IsPermissionError`, `ValidateConfiguration`, `QueryCatalog`, etc.), even if some return `ErrUnsupported`. For each method with nontrivial SQL, implement it in a separate file (e.g., `query_catalog.go`) and put the SQL in a separate `.sql` file loaded at runtime, as in the postgres example. If a dialect does not exist, return nil or a TODO stub.
- Implement `Close()` to close the executor.

## General

- Follow the dialect and executor patterns from similar DWHs.
- Register the new type wherever DWHs are enumerated (if needed).
