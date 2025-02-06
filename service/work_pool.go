package service

import (
	"sync"

	agentdwhv1grpc "github.com/getsynq/api/agent/dwh/v1"
	ingestdwhv1 "github.com/getsynq/api/ingest/dwh/v1"
)

// WorkPool manages multiple workers for different database connections
type WorkPool struct {
	workers map[string]*Worker
	mu      sync.RWMutex
}

// NewWorkPool creates a new work pool with workers for each database connection
func NewWorkPool(databaseConnections map[string]*agentdwhv1grpc.Config_Connection, dwhServiceClient ingestdwhv1.DwhServiceClient) *WorkPool {
	wp := &WorkPool{
		workers: make(map[string]*Worker),
	}

	// Create a worker for each database connection
	for dbID, dbConf := range databaseConnections {
		if dbConf.Disabled {
			continue
		}
		worker := &Worker{
			connectionId: dbID,
			parallelism:  int(dbConf.Parallelism),
			msgChan:      make(chan DatabaseCommand, 10), // Buffer size of 100
			done:         make(chan struct{}),
			conf:         dbConf,
			publisher:    NewPublisher(dwhServiceClient),
		}
		wp.workers[dbID] = worker
		go worker.start()
	}

	return wp
}

// GetWorker returns the worker for the specified database ID
func (wp *WorkPool) GetWorker(databaseID string) *Worker {
	wp.mu.RLock()
	defer wp.mu.RUnlock()
	return wp.workers[databaseID]
}

// Stop stops all workers in the pool
func (wp *WorkPool) Stop() {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	for _, worker := range wp.workers {
		close(worker.done)
	}
}
