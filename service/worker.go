package service

import (
	"context"
	"sync"
	"time"

	agentdwhv1 "github.com/getsynq/api/agent/dwh/v1"
	dwhexecbigquery "github.com/getsynq/dwhsupport/exec/bigquery"
	dwhexecclickhouse "github.com/getsynq/dwhsupport/exec/clickhouse"
	dwhexecdatabricks "github.com/getsynq/dwhsupport/exec/databricks"
	dwhexecredshift "github.com/getsynq/dwhsupport/exec/redshift"
	dwhexecsnowflake "github.com/getsynq/dwhsupport/exec/snowflake"
	scrapper "github.com/getsynq/dwhsupport/scrapper"
	scrapperbigquery "github.com/getsynq/dwhsupport/scrapper/bigquery"
	scrapperclickhouse "github.com/getsynq/dwhsupport/scrapper/clickhouse"
	scrapperdatabricks "github.com/getsynq/dwhsupport/scrapper/databricks"
	scrappermysql "github.com/getsynq/dwhsupport/scrapper/mysql"
	scrapperpostgres "github.com/getsynq/dwhsupport/scrapper/postgres"
	scrapperredshift "github.com/getsynq/dwhsupport/scrapper/redshift"
	scrappersnowflake "github.com/getsynq/dwhsupport/scrapper/snowflake"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

// Worker represents a database-specific worker that processes messages
type Worker struct {
	connectionId        string
	msgChan             chan DatabaseCommand
	done                chan struct{}
	conf                *agentdwhv1.Config_Connection
	scrapper            scrapper.Scrapper
	errors              int
	ctx                 context.Context
	cancel              context.CancelFunc
	lastMetricFetchTime time.Time
	parallelism         int
	publisher           *Publisher
}

// EnqueueMessage adds a message to the worker's queue
func (w *Worker) EnqueueMessage(msg DatabaseCommand) {
	select {
	case w.msgChan <- msg:
	default:
		// Queue is full, log error or handle appropriately
		logrus.Errorf("Worker queue full for database ID: %s", w.connectionId)
	}
}

// start begins processing messages for this worker
func (w *Worker) start() {
	ctx, cancel := context.WithCancel(context.Background())
	w.ctx = ctx
	w.cancel = cancel

	// Create timer for connection timeout
	connectionTimer := time.NewTimer(30 * time.Second)
	defer connectionTimer.Stop()

	// Create a semaphore to limit concurrent operations
	sem := make(chan struct{}, w.parallelism)
	// Add a WaitGroup to track active operations
	var wg sync.WaitGroup

	// Track number of active operations
	activeOps := 0

	for {
		// Reset timer if it's running
		if !connectionTimer.Stop() {
			select {
			case <-connectionTimer.C:
			default:
			}
		}
		connectionTimer.Reset(30 * time.Second)

		select {
		case <-w.done:
			// Cancel context to signal operations to stop
			w.cancel()

			// Drain message channel to prevent goroutine leaks
			go func() {
				for range w.msgChan {
					// Drain remaining messages
				}
			}()

			// Wait for all operations to complete
			wg.Wait()

			// Close database connection
			if w.scrapper != nil {
				w.scrapper.Close()
				w.scrapper = nil
			}
			return
		case <-connectionTimer.C:
			// Only close connection if there are no active operations
			if w.scrapper != nil && activeOps == 0 {
				logrus.Infof("Closing idle database connection for %s", w.connectionId)
				w.scrapper.Close()
				w.scrapper = nil
			}
		case msg := <-w.msgChan:
			if w.scrapper == nil {
				logrus.Infof("Connecting to database %s", w.connectionId)
				scrapper, err := w.connect()
				w.errors += 1
				if err != nil {
					logrus.Errorf("Error connecting to database %s: %v (try %d)", w.connectionId, err, w.errors+1)
					time.Sleep(reconnectDelay)
					select {
					case w.msgChan <- msg:
					default:
						logrus.Errorf("Worker queue full for database ID: %s", w.connectionId)
					}
					continue
				}
				w.scrapper = scrapper
			}

			// Acquire semaphore slot
			select {
			case sem <- struct{}{}:
			case <-w.done:
				return
			}

			// Increment active operations counter
			activeOps++

			// Launch goroutine to handle the command
			wg.Add(1)
			go func(msg DatabaseCommand) {
				defer wg.Done() // Decrement WaitGroup when done
				defer func() {
					<-sem       // Release semaphore slot
					activeOps-- // Decrement active operations counter
				}()

				logrus.Infof("Processing command for database %s: %T", w.connectionId, msg)
				switch msg.(type) {
				case *FetchFullCatalog:
					ctx, c := context.WithTimeout(w.ctx, 55*time.Minute)
					defer c()

					stateAt := time.Now().UTC()

					tableRows, err := w.scrapper.QueryTables(ctx)
					if err != nil {
						logrus.Errorf("Error fetching tables for database %s: %v", w.connectionId, err)
						return
					}
					logrus.Infof("Fetched %d tables for database %s", len(tableRows), w.connectionId)

					catalogRows, err := w.scrapper.QueryCatalog(ctx)
					if err != nil {
						logrus.Errorf("Error fetching columns for database %s: %v", w.connectionId, err)
						return
					}
					logrus.Infof("Fetched %d columns for database %s", len(catalogRows), w.connectionId)

					sqlDefinitionsRows, err := w.scrapper.QuerySqlDefinitions(ctx)
					if err != nil {
						logrus.Errorf("Error fetching sql definitions for database %s: %v", w.connectionId, err)
						return
					}
					logrus.Infof("Fetched %d sql definitions for database %s", len(sqlDefinitionsRows), w.connectionId)

					err = w.publisher.PublishCatalog(ctx, w.connectionId, stateAt, tableRows, catalogRows, sqlDefinitionsRows)
					if err != nil {
						logrus.Errorf("Error publishing catalog for database %s: %v", w.connectionId, err)
						return
					}

				case *FetchFullMetrics:
					lastMetricFetchTime := time.Now().UTC()
					ctx, c := context.WithTimeout(w.ctx, 55*time.Minute)
					defer c()

					stateAt := time.Now().UTC()

					metricRows, err := w.scrapper.QueryTableMetrics(ctx, w.lastMetricFetchTime)
					if err != nil {
						logrus.Errorf("Error fetching metrics for database %s: %v", w.connectionId, err)
						return
					}
					logrus.Infof("Fetched %d metrics for database %s", len(metricRows), w.connectionId)

					err = w.publisher.PublishMetrics(ctx, w.connectionId, stateAt, metricRows)
					if err != nil {
						logrus.Errorf("Error publishing metrics for database %s: %v", w.connectionId, err)
						return
					}

					w.lastMetricFetchTime = lastMetricFetchTime
				}
			}(msg)
		}
	}
}

func (w *Worker) connect() (scrapper.Scrapper, error) {
	switch t := w.conf.Config.(type) {
	case *agentdwhv1.Config_Connection_Bigquery:
		return scrapperbigquery.NewBigQueryScrapper(w.ctx, &scrapperbigquery.BigQueryScrapperConf{
			BigQueryConf: dwhexecbigquery.BigQueryConf{
				ProjectId:       t.Bigquery.GetProjectId(),
				Region:          t.Bigquery.GetRegion(),
				CredentialsJson: t.Bigquery.GetServiceAccountKey(),
				CredentialsFile: t.Bigquery.GetServiceAccountKeyFile(),
			},
		})

	case *agentdwhv1.Config_Connection_Clickhouse:
		return scrapperclickhouse.NewClickhouseScrapper(w.ctx, scrapperclickhouse.ClickhouseScrapperConf{
			ClickhouseConf: dwhexecclickhouse.ClickhouseConf{
				Hostname:        t.Clickhouse.GetHost(),
				Port:            int(t.Clickhouse.GetPort()),
				Username:        t.Clickhouse.GetUsername(),
				Password:        t.Clickhouse.GetPassword(),
				DefaultDatabase: t.Clickhouse.GetDatabase(),
				NoSsl:           t.Clickhouse.GetAllowInsecure(),
			},
		})

	case *agentdwhv1.Config_Connection_Databricks:
		var auth dwhexecdatabricks.Auth
		if t.Databricks.AuthToken != nil {
			auth = dwhexecdatabricks.NewTokenAuth(t.Databricks.GetAuthToken())
		} else {
			auth = dwhexecdatabricks.NewOAuthM2mAuth(t.Databricks.GetAuthClient(), t.Databricks.GetAuthSecret())
		}

		return scrapperdatabricks.NewDatabricksScrapper(w.ctx, &scrapperdatabricks.DatabricksScrapperConf{
			DatabricksConf: dwhexecdatabricks.DatabricksConf{
				WorkspaceUrl: t.Databricks.GetWorkspaceUrl(),
				Auth:         auth,
				WarehouseId:  t.Databricks.GetWarehouse(),
			},
			RefreshTableMetrics:        t.Databricks.GetRefreshTableMetrics(),
			RefreshTableMetricsUseScan: t.Databricks.GetRefreshTableMetricsUseScan(),
			FetchTableTags:             t.Databricks.GetFetchTableTags(),
			UseShowCreateTable:         t.Databricks.GetUseShowCreateTable(),
		})

	case *agentdwhv1.Config_Connection_Mysql:
		return scrappermysql.NewMySQLScrapper(w.ctx, &scrappermysql.MySQLScrapperConf{
			User:     t.Mysql.GetUsername(),
			Password: t.Mysql.GetPassword(),
			Host:     t.Mysql.GetHost(),
			Port:     int(t.Mysql.GetPort()),
		})

	case *agentdwhv1.Config_Connection_Postgres:
		return scrapperpostgres.NewPostgresScrapper(w.ctx, &scrapperpostgres.PostgresScapperConf{
			User:          t.Postgres.GetUsername(),
			Password:      t.Postgres.GetPassword(),
			Database:      t.Postgres.GetDatabase(),
			Host:          t.Postgres.GetHost(),
			Port:          int(t.Postgres.GetPort()),
			AllowInsecure: t.Postgres.GetAllowInsecure(),
		})
	case *agentdwhv1.Config_Connection_Redshift:
		return scrapperredshift.NewRedshiftScrapper(w.ctx, &scrapperredshift.RedshiftScrapperConf{
			RedshiftConf: dwhexecredshift.RedshiftConf{
				User:     t.Redshift.GetUsername(),
				Password: t.Redshift.GetPassword(),
				Database: t.Redshift.GetDatabase(),
				Host:     t.Redshift.GetHost(),
				Port:     int(t.Redshift.GetPort()),
			},
			FreshnessFromQueryLogs: t.Redshift.GetFreshnessFromQueryLogs(),
		})
	case *agentdwhv1.Config_Connection_Snowflake:
		return scrappersnowflake.NewSnowflakeScrapper(w.ctx, &scrappersnowflake.SnowflakeScrapperConf{
			SnowflakeConf: dwhexecsnowflake.SnowflakeConf{
				User:       t.Snowflake.GetUsername(),
				Password:   t.Snowflake.GetPassword(),
				PrivateKey: []byte(t.Snowflake.GetPrivateKey()),
				Account:    t.Snowflake.GetAccount(),
				Warehouse:  t.Snowflake.GetWarehouse(),
				Databases:  t.Snowflake.GetDatabases(),
				Role:       t.Snowflake.GetRole(),
			},
			NoGetDll: !t.Snowflake.GetUseGetDdl(),
		})

	default:
		return nil, errors.New("unsupported database type")
	}
}
