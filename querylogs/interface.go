package querylogs

import (
	"context"
	"io"
	"time"
)

// QueryLogIterator provides sequential access to query logs.
// Implementations should fetch from the data warehouse as fast as possible
// and buffer internally to minimize warehouse connection time.
//
// IMPORTANT: Implementations MUST automatically close resources when returning io.EOF
// from Next(). This ensures no resource leaks even if caller forgets to call Close().
type QueryLogIterator interface {
	// Next returns the next query log from the iterator.
	//
	// Returns:
	//   - The next QueryLog and nil error on success
	//   - nil and io.EOF when no more logs are available
	//   - nil and an error if fetching fails
	//
	// Implementation notes:
	//   - Should fetch from warehouse in optimal batch sizes internally
	//   - Should buffer results to minimize warehouse connection time
	//   - Must respect context cancellation
	//   - MUST auto-close all resources before returning io.EOF (defensive programming)
	//   - Safe to call multiple times after io.EOF
	Next(ctx context.Context) (*QueryLog, error)

	// Close releases any resources held by the iterator.
	// Should be called when iteration is complete or abandoned, but implementations
	// MUST also auto-close when Next() returns io.EOF to prevent resource leaks.
	// Safe to call multiple times.
	Close() error
}

// QueryLogsProvider defines the interface for fetching query logs from data warehouse platforms.
type QueryLogsProvider interface {
	// FetchQueryLogs returns an iterator for query logs within the specified time range.
	//
	// Parameters:
	//   - ctx: Context for cancellation and deadlines
	//   - from: Start of the time range (inclusive)
	//   - to: End of the time range (exclusive)
	//
	// Returns:
	//   - An iterator for sequential access to query logs
	//   - The caller is responsible for calling Close() on the iterator
	//
	// Implementation notes:
	//   - Iterator should fetch from warehouse as fast as possible
	//   - Platform-specific optimizations (batch sizes, parallel fetching) should happen inside the iterator
	//   - Query logs should be ordered by CreatedAt timestamp when possible
	//   - Context cancellation should be respected throughout iteration
	//
	// Example usage:
	//   iter, err := provider.FetchQueryLogs(ctx, from, to)
	//   if err != nil {
	//       return err
	//   }
	//   defer iter.Close()
	//
	//   for {
	//       log, err := iter.Next(ctx)
	//       if err == io.EOF {
	//           break
	//       }
	//       if err != nil {
	//           return err
	//       }
	//       // Process log...
	//   }
	FetchQueryLogs(ctx context.Context, from, to time.Time) (QueryLogIterator, error)
}
