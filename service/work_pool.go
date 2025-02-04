package service

import (
	"context"
	"sync"

	agentdwhv1grpc "github.com/getsynq/api/agent/dwh/v1"
	dwhpostgres "github.com/getsynq/dwhsupport/scrapper/postgres"
	"github.com/sirupsen/logrus"
)

// Worker represents a database-specific worker that processes messages
type Worker struct {
	databaseID string
	msgChan    chan interface{}
	done       chan struct{}
	conf       *agentdwhv1grpc.Config_Connection
}

// WorkPool manages multiple workers for different database connections
type WorkPool struct {
	workers map[string]*Worker
	mu      sync.RWMutex
}

// NewWorkPool creates a new work pool with workers for each database connection
func NewWorkPool(databaseConnections map[string]*agentdwhv1grpc.Config_Connection) *WorkPool {
	wp := &WorkPool{
		workers: make(map[string]*Worker),
	}

	// Create a worker for each database connection
	for dbID, dbConf := range databaseConnections {
		worker := &Worker{
			databaseID: dbID,
			msgChan:    make(chan interface{}, 100), // Buffer size of 100
			done:       make(chan struct{}),
			conf:       dbConf,
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

// EnqueueMessage adds a message to the worker's queue
func (w *Worker) EnqueueMessage(msg interface{}) {
	select {
	case w.msgChan <- msg:
	default:
		// Queue is full, log error or handle appropriately
		logrus.Errorf("Worker queue full for database ID: %s", w.databaseID)
	}
}

// start begins processing messages for this worker
func (w *Worker) start() {
	for {
		select {
		case <-w.done:
			return
		case msg := <-w.msgChan:
			// Process the message
			// TODO: Implement actual message processing logic

			exec, err := dwhpostgres.NewPostgresScrapper(context.TODO(), &dwhpostgres.PostgresScapperConf{
				User:          "",
				Password:      "",
				Database:      "",
				Host:          "",
				Port:          0,
				AllowInsecure: false,
				SSHTunnel:     nil,
			})
			if err != nil {
				logrus.Errorf("Error creating postgres scrapper: %v", err)
				continue
			}
			defer exec.Close()

			logrus.Infof("Processing message for database %s: %v", w.databaseID, msg)
		}
	}
}
