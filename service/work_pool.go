package service

import (
	"context"
	"sync"
	"time"

	agentdwhv1grpc "github.com/getsynq/api/agent/dwh/v1"
	dwhscrapper "github.com/getsynq/dwhsupport/scrapper"
	dwhpostgres "github.com/getsynq/dwhsupport/scrapper/postgres"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Worker represents a database-specific worker that processes messages
type Worker struct {
	databaseID          string
	msgChan             chan DatabaseCommand
	done                chan struct{}
	conf                *agentdwhv1grpc.Config_Connection
	scrapper            dwhscrapper.Scrapper
	errors              int
	ctx                 context.Context
	cancel              context.CancelFunc
	lastMetricFetchTime time.Time
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
			msgChan:    make(chan DatabaseCommand, 10), // Buffer size of 100
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
func (w *Worker) EnqueueMessage(msg DatabaseCommand) {
	select {
	case w.msgChan <- msg:
	default:
		// Queue is full, log error or handle appropriately
		logrus.Errorf("Worker queue full for database ID: %s", w.databaseID)
	}
}

// start begins processing messages for this worker
func (w *Worker) start() {
	ctx, cancel := context.WithCancel(context.Background())
	w.ctx = ctx
	w.cancel = cancel
	for {
		select {
		case <-w.done:
			w.cancel()
			return
		case msg := <-w.msgChan:
			if w.scrapper == nil {
				logrus.Infof("Connecting to database %s", w.databaseID)
				scrapper, err := w.connect()
				w.errors += 1
				if err != nil {
					logrus.Errorf("Error connecting to database %s: %v (try %d)", w.databaseID, err, w.errors+1)
					time.Sleep(reconnectDelay)
					select {
					case w.msgChan <- msg:
					default:
						logrus.Errorf("Worker queue full for database ID: %s", w.databaseID)
					}
					continue
				}
				w.scrapper = scrapper
			}
			logrus.Infof("Processing command for database %s: %T", w.databaseID, msg)
			switch msg.(type) {
			case *FetchFullCatalog:
				ctx, c := context.WithTimeout(w.ctx, 55*time.Minute)
				catalogRows, err := w.scrapper.QueryCatalog(ctx)
				c()
				if err != nil {
					logrus.Errorf("Error fetching catalog for database %s: %v", w.databaseID, err)
					continue
				}
				logrus.Infof("Fetched %d catalog entries for database %s", len(catalogRows), w.databaseID)

			case *FetchFullMetrics:
				lastMetricFetchTime := time.Now().UTC()
				ctx, c := context.WithTimeout(w.ctx, 55*time.Minute)
				metricRows, err := w.scrapper.QueryTableMetrics(ctx, w.lastMetricFetchTime)
				c()
				if err != nil {
					logrus.Errorf("Error fetching catalog for database %s: %v", w.databaseID, err)
					continue
				}
				logrus.Infof("Fetched %d metrics for database %s", len(metricRows), w.databaseID)

				w.lastMetricFetchTime = lastMetricFetchTime
			}
		}
	}
}

func (w *Worker) connect() (dwhscrapper.Scrapper, error) {
	if w.conf.Postgres != nil {
		return dwhpostgres.NewPostgresScrapper(w.ctx, &dwhpostgres.PostgresScapperConf{
			User:          w.conf.Postgres.GetUsername(),
			Password:      w.conf.Postgres.GetPassword(),
			Database:      w.conf.Postgres.GetDatabase(),
			Host:          w.conf.Postgres.GetHost(),
			Port:          int(w.conf.Postgres.GetPort()),
			AllowInsecure: w.conf.Postgres.GetAllowInsecure(),
		})
	}
	return nil, errors.New("unsupported database type")
}
