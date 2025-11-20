package querylogs

import (
	"context"
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

// QueryObfuscator provides SQL obfuscation for query logs.
//
// Platforms can handle obfuscation in two ways:
// 1. Native obfuscation (ClickHouse): Check Mode(), if not ObfuscationNone, use native SQL functions
// 2. Post-processing (all platforms): Call Obfuscate(sql) - it handles obfuscation based on Mode()
//
// The obfuscator is always required. Check Mode() to decide whether to use native obfuscation,
// then always call Obfuscate() for post-processing.
type QueryObfuscator interface {
	// Mode returns the obfuscation mode configured for this obfuscator.
	// Platforms with native obfuscation support should check this to decide
	// whether to use native SQL functions (e.g., normalizeQuery() in ClickHouse).
	Mode() ObfuscationMode

	// Obfuscate processes SQL text and returns obfuscated version based on Mode().
	// If Mode() is ObfuscationNone, returns SQL unchanged.
	// Otherwise applies configured obfuscation (literals, bind parameters, etc.)
	Obfuscate(sql string) string
}

// QueryLogsProvider defines the interface for fetching query logs from data warehouse platforms.
type QueryLogsProvider interface {
	// FetchQueryLogs returns an iterator for query logs within the specified time range.
	//
	// Parameters:
	//   - ctx: Context for cancellation and deadlines
	//   - from: Start of the time range (inclusive)
	//   - to: End of the time range (exclusive)
	//   - obfuscator: Controls SQL obfuscation mode and provides helper function
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
	//   - Platforms should handle obfuscation optimally:
	//       * Use native SQL functions when available (e.g., ClickHouse normalizeQuery())
	//       * Use obfuscator.Obfuscate() helper for post-processing when native isn't available
	//       * Skip obfuscation entirely if obfuscator.Mode() == ObfuscationNone
	//
	// Example usage:
	//   obfuscator := querylogs.NewLiteralsObfuscator()
	//   iter, err := provider.FetchQueryLogs(ctx, from, to, obfuscator)
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
	FetchQueryLogs(ctx context.Context, from, to time.Time, obfuscator QueryObfuscator) (QueryLogIterator, error)
}
