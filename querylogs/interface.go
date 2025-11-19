package querylogs

import (
	"context"
	"time"
)

// QueryLogsProvider defines the interface for fetching query logs from data warehouse platforms.
// Implementations should stream query logs in batches to the provided receiver function.
type QueryLogsProvider interface {
	// FetchQueryLogs retrieves query logs within the specified time range and streams them to the receiver.
	//
	// Parameters:
	//   - ctx: Context for cancellation and deadlines
	//   - from: Start of the time range (inclusive)
	//   - to: End of the time range (exclusive)
	//   - receiver: Callback function that receives batches of query logs
	//
	// The receiver function is called multiple times with batches of query logs. It should return an error
	// if processing fails, which will stop the iteration and return the error from FetchQueryLogs.
	//
	// Implementation notes:
	//   - Query logs should be ordered by CreatedAt timestamp
	//   - Batch size is implementation-specific but should balance memory usage and callback overhead
	//   - The receiver may be called with empty slices in some implementations
	//   - Context cancellation should be respected and stop the fetching process
	FetchQueryLogs(
		ctx context.Context,
		from, to time.Time,
		receiver func(ctx context.Context, logs []*QueryLog) error,
	) error
}
