# Rule for Adding a New Executer and Scrapper

## Executer

- Define a config struct for your DWH (e.g., `FoobarConf`).
- Implement an `Executor` interface (usually embeds or implements `stdsql.StdSqlExecutor` or similar).
- Implement a struct (e.g., `FoobarExecutor`) with connection logic, `GetDb()`, `QueryRows()`, `Exec()`, `Close()`.
- Provide a constructor (e.g., `NewFoobarExecutor(ctx, conf)`).
- Implement `NewQuerier[T any](conn *FoobarExecutor) querier.Querier[T]` returning a `stdsql.NewQuerier[T](conn.db)` or similar.
- Implement `query.go` with `NewQuerier[T any](conn *FoobarExecutor) querier.Querier[T]` returning a `stdsql.NewQuerier[T](conn.db)` or similar.

## Scrapper

- Define a config struct for your scrapper, often reusing the exec config.
- Implement the `Scrapper` interface from `scrapper/interface.go`.
- Implement a struct (e.g., `FoobarScrapper`) with fields for config and executor.
- Provide a constructor (e.g., `NewFoobarScrapper(ctx, conf)`), which instantiates the executor.
- Implement all required methods (`DialectType`, `SqlDialect`, `IsPermissionError`, `ValidateConfiguration`, `QueryCatalog`, etc.), even if some return `ErrUnsupported`.
- Implement `Close()` to close the executor.

## General

- Follow the dialect and executor patterns from similar DWHs.
- Register the new type wherever DWHs are enumerated (if needed).
