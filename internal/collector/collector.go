package collector

import (
	"context"
	"time"

	"github.com/everton/airflow-exporter/internal/airflow"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

type Collector struct {
	client *airflow.Client
	logger *zap.Logger
	meter  metric.Meter

	// Metrics
	up                   metric.Int64Gauge
	scrapeDuration       metric.Float64Histogram
	dagRunState          metric.Int64Gauge
	taskInstanceState    metric.Int64Gauge
	operatorFailures     metric.Int64Gauge
	dagRunDurationAvg24h metric.Float64Gauge
	taskDurationAvg24h   metric.Float64Gauge
	taskQueueWaitAvg24h  metric.Float64Gauge
	dagActive            metric.Int64Gauge

	// State tracking
	dagRunStateKeys       map[attribute.Set]struct{}
	taskInstanceStateKeys map[attribute.Set]struct{}
}

func New(client *airflow.Client, logger *zap.Logger, meter metric.Meter) (*Collector, error) {
	c := &Collector{
		client:                client,
		logger:                logger,
		meter:                 meter,
		dagRunStateKeys:       make(map[attribute.Set]struct{}),
		taskInstanceStateKeys: make(map[attribute.Set]struct{}),
	}

	var err error
	if c.up, err = meter.Int64Gauge("airflow_exporter_up", metric.WithDescription("1 if the scrape was successful")); err != nil {
		return nil, err
	}
	if c.scrapeDuration, err = meter.Float64Histogram("airflow_exporter_scrape_duration_seconds", metric.WithDescription("Duration of the DB scrape")); err != nil {
		return nil, err
	}
	if c.dagRunState, err = meter.Int64Gauge("airflow_dag_run_state", metric.WithDescription("Snapshot count of DAG runs in each state")); err != nil {
		return nil, err
	}
	if c.taskInstanceState, err = meter.Int64Gauge("airflow_task_instance_state", metric.WithDescription("Snapshot count of Task instances in each state")); err != nil {
		return nil, err
	}
	if c.operatorFailures, err = meter.Int64Gauge("airflow_task_operator_failures", metric.WithDescription("Count of FAILED tasks grouped by Operator type")); err != nil {
		return nil, err
	}
	if c.dagRunDurationAvg24h, err = meter.Float64Gauge("airflow_dag_run_duration_avg_24h", metric.WithDescription("Average duration of finished DAGs in the last 24h")); err != nil {
		return nil, err
	}
	if c.dagActive, err = meter.Int64Gauge("airflow_dag_active", metric.WithDescription("Count of DAGs by active status (active/paused)")); err != nil {
		return nil, err
	}
	if c.taskDurationAvg24h, err = meter.Float64Gauge("airflow_task_duration_avg_24h", metric.WithDescription("Average duration of finished Tasks in the last 24h")); err != nil {
		return nil, err
	}
	if c.taskQueueWaitAvg24h, err = meter.Float64Gauge("airflow_task_queue_wait_duration_avg_24h", metric.WithDescription("Average start delay of Tasks in the last 24h")); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *Collector) Start(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	// Initial Scrape
	c.Scrape(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.Scrape(ctx)
		}
	}
}

func (c *Collector) Scrape(ctx context.Context) {
	start := time.Now()
	success := true

	defer func() {
		duration := time.Since(start).Seconds()
		c.scrapeDuration.Record(ctx, duration)
		if success {
			c.up.Record(ctx, 1)
		} else {
			c.up.Record(ctx, 0)
		}
	}()

	// 1. DAG Runs
	dagStates, err := c.client.GetDagRunStates(ctx)
	if err != nil {
		c.logger.Error("Failed to scrape DAG runs", zap.Error(err))
		success = false
	} else {

		newKeys := make(map[attribute.Set]struct{})
		for _, m := range dagStates {
			attrs := attribute.NewSet(
				attribute.String("repository", m.Repository),
				attribute.String("state", m.Label),
			)
			c.dagRunState.Record(ctx, int64(m.Count), metric.WithAttributeSet(attrs))
			newKeys[attrs] = struct{}{}
		}
		// Reset missing keys to 0
		for k := range c.dagRunStateKeys {
			if _, ok := newKeys[k]; !ok {
				c.dagRunState.Record(ctx, 0, metric.WithAttributeSet(k))
			}
		}
		c.dagRunStateKeys = newKeys
	}

	// 2. Task Instances
	taskStates, err := c.client.GetTaskInstanceStates(ctx)
	if err != nil {
		c.logger.Error("Failed to scrape Task instances", zap.Error(err))
		success = false
	} else {
		newTaskKeys := make(map[attribute.Set]struct{})
		for _, m := range taskStates {
			attrs := attribute.NewSet(
				attribute.String("repository", m.Repository),
				attribute.String("state", m.Label),
			)
			c.taskInstanceState.Record(ctx, int64(m.Count), metric.WithAttributeSet(attrs))
			newTaskKeys[attrs] = struct{}{}
		}
		// Reset missing keys to 0
		for k := range c.taskInstanceStateKeys {
			if _, ok := newTaskKeys[k]; !ok {
				c.taskInstanceState.Record(ctx, 0, metric.WithAttributeSet(k))
			}
		}
		c.taskInstanceStateKeys = newTaskKeys
	}

	// 3. Operator Failures
	opFailures, err := c.client.GetOperatorFailures(ctx)
	if err != nil {
		c.logger.Error("Failed to scrape Operator failures", zap.Error(err))
		success = false
	} else {
		for _, m := range opFailures {
			c.operatorFailures.Record(ctx, int64(m.Count), metric.WithAttributes(
				attribute.String("repository", m.Repository),
				attribute.String("operator", m.Label),
			))
		}
	}

	// 4. Durations
	durations, err := c.client.GetDagRunDurations(ctx)
	if err != nil {
		c.logger.Error("Failed to scrape durations", zap.Error(err))
		success = false
	} else {
		for _, m := range durations {
			c.dagRunDurationAvg24h.Record(ctx, m.Duration, metric.WithAttributes(
				attribute.String("repository", m.Repository),
			))
		}
	}

	// 5. Task Durations
	taskDurations, err := c.client.GetTaskDurations(ctx)
	if err != nil {
		c.logger.Error("Failed to scrape task durations", zap.Error(err))
		success = false
	} else {
		for _, m := range taskDurations {
			c.taskDurationAvg24h.Record(ctx, m.Duration, metric.WithAttributes(
				attribute.String("repository", m.Repository),
			))
		}
	}

	// 6. Task Queue Wait
	taskWaits, err := c.client.GetTaskQueueWait(ctx)
	if err != nil {
		c.logger.Error("Failed to scrape task queue wait", zap.Error(err))
		success = false
	} else {
		for _, m := range taskWaits {
			c.taskQueueWaitAvg24h.Record(ctx, m.Duration, metric.WithAttributes(
				attribute.String("repository", m.Repository),
			))
		}
	}
}
