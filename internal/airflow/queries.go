package airflow

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Client struct {
	db *pgxpool.Pool
}

func NewClient(db *pgxpool.Pool) *Client {
	return &Client{db: db}
}

type MetricCount struct {
	Repository string
	Label      string // reused for state or operator
	Count      int
}

type DurationMetric struct {
	Repository string
	Duration   float64
}

type PoolMetric struct {
	Pool       string
	TotalSlots int
	UsedSlots  int
}

// GetDagRunStates counts dag runs per state and repository
func (c *Client) GetDagRunStates(ctx context.Context) ([]MetricCount, error) {
	// Assumption: fileloc contains "/opt/airflow/dags/<repository>/..."
	// We extract the <repository> part.
	sql := `
		SELECT 
			SUBSTRING(d.fileloc FROM '/opt/airflow/dags/([^/]+)/') as repository,
			dr.state,
			COUNT(*) 
		FROM dag_run dr
		JOIN dag d ON dr.dag_id = d.dag_id
		GROUP BY 1, 2
	`
	rows, err := c.db.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var results []MetricCount
	for rows.Next() {
		var r MetricCount
		var repo *string
		var label *string
		if err := rows.Scan(&repo, &label, &r.Count); err != nil {
			return nil, err
		}
		if repo != nil {
			r.Repository = *repo
		} else {
			r.Repository = "unknown"
		}
		if label != nil {
			r.Label = *label
		} else {
			r.Label = "none"
		}
		results = append(results, r)
	}
	return results, nil
}

// GetTaskInstanceStates counts task instances per state and repository
func (c *Client) GetTaskInstanceStates(ctx context.Context) ([]MetricCount, error) {
	sql := `
		SELECT 
			SUBSTRING(d.fileloc FROM '/opt/airflow/dags/([^/]+)/') as repository,
			ti.state,
			COUNT(*) 
		FROM task_instance ti
		JOIN dag d ON ti.dag_id = d.dag_id
		GROUP BY 1, 2
	`
	rows, err := c.db.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var results []MetricCount
	for rows.Next() {
		var r MetricCount
		var repo *string
		var label *string
		if err := rows.Scan(&repo, &label, &r.Count); err != nil {
			return nil, err
		}
		if repo != nil {
			r.Repository = *repo
		} else {
			r.Repository = "unknown"
		}
		if label != nil {
			r.Label = *label
		} else {
			r.Label = "none"
		}
		results = append(results, r)
	}
	return results, nil
}

// GetOperatorFailures counts failed tasks per operator and repository
func (c *Client) GetOperatorFailures(ctx context.Context) ([]MetricCount, error) {
	sql := `
		SELECT 
			SUBSTRING(d.fileloc FROM '/opt/airflow/dags/([^/]+)/') as repository,
			ti.operator,
			COUNT(*) 
		FROM task_instance ti
		JOIN dag d ON ti.dag_id = d.dag_id
		WHERE ti.state = 'failed'
		GROUP BY 1, 2
	`
	rows, err := c.db.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var results []MetricCount
	for rows.Next() {
		var r MetricCount
		var repo *string
		var label *string
		if err := rows.Scan(&repo, &label, &r.Count); err != nil {
			return nil, err
		}
		if repo != nil {
			r.Repository = *repo
		} else {
			r.Repository = "unknown"
		}
		if label != nil {
			r.Label = *label
		} else {
			r.Label = "none"
		}
		results = append(results, r)
	}
	return results, nil
}

// GetDagRunDurations calculates avg duration of finished DAGs in last 24h
func (c *Client) GetDagRunDurations(ctx context.Context) ([]DurationMetric, error) {
	sql := `
		SELECT 
			SUBSTRING(d.fileloc FROM '/opt/airflow/dags/([^/]+)/') as repository,
			AVG(EXTRACT(EPOCH FROM (dr.end_date - dr.start_date)))
		FROM dag_run dr
		JOIN dag d ON dr.dag_id = d.dag_id
		WHERE dr.state IN ('success', 'failed') 
		  AND dr.end_date > NOW() - INTERVAL '24 hours'
		GROUP BY 1
	`
	rows, err := c.db.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var results []DurationMetric
	for rows.Next() {
		var r DurationMetric
		var repo *string
		if err := rows.Scan(&repo, &r.Duration); err != nil {
			return nil, err
		}
		if repo != nil {
			r.Repository = *repo
		} else {
			r.Repository = "unknown"
		}
		results = append(results, r)
	}
	return results, nil
}

// GetTaskDurations calculates avg duration of finished tasks in last 24h
func (c *Client) GetTaskDurations(ctx context.Context) ([]DurationMetric, error) {
	sql := `
		SELECT 
			SUBSTRING(d.fileloc FROM '/opt/airflow/dags/([^/]+)/') as repository,
			AVG(EXTRACT(EPOCH FROM (ti.end_date - ti.start_date)))
		FROM task_instance ti
		JOIN dag d ON ti.dag_id = d.dag_id
		WHERE ti.state IN ('success', 'failed') 
		  AND ti.end_date > NOW() - INTERVAL '24 hours'
		GROUP BY 1
	`
	rows, err := c.db.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var results []DurationMetric
	for rows.Next() {
		var r DurationMetric
		var repo *string
		if err := rows.Scan(&repo, &r.Duration); err != nil {
			return nil, err
		}
		if repo != nil {
			r.Repository = *repo
		} else {
			r.Repository = "unknown"
		}
		results = append(results, r)
	}
	return results, nil
}

// GetTaskQueueWait calculates avg queue wait time for tasks started in last 24h
func (c *Client) GetTaskQueueWait(ctx context.Context) ([]DurationMetric, error) {
	sql := `
		SELECT 
			SUBSTRING(d.fileloc FROM '/opt/airflow/dags/([^/]+)/') as repository,
			AVG(GREATEST(EXTRACT(EPOCH FROM (ti.start_date - ti.queued_dttm)), 0))
		FROM task_instance ti
		JOIN dag d ON ti.dag_id = d.dag_id
		WHERE ti.start_date > NOW() - INTERVAL '24 hours'
		  AND ti.queued_dttm IS NOT NULL
		GROUP BY 1
	`
	rows, err := c.db.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var results []DurationMetric
	for rows.Next() {
		var r DurationMetric
		var repo *string
		if err := rows.Scan(&repo, &r.Duration); err != nil {
			return nil, err
		}
		if repo != nil {
			r.Repository = *repo
		} else {
			r.Repository = "unknown"
		}
		results = append(results, r)
	}
	return results, nil
}

// GetDagParseDurations calculates the average DAG parse duration per repository
func (c *Client) GetDagParseDurations(ctx context.Context) ([]DurationMetric, error) {
	sql := `
		SELECT 
			SUBSTRING(fileloc FROM '/opt/airflow/dags/([^/]+)/') as repository,
			AVG(last_parse_duration)
		FROM dag
		WHERE last_parse_duration IS NOT NULL
		GROUP BY 1
	`
	rows, err := c.db.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var results []DurationMetric
	for rows.Next() {
		var r DurationMetric
		var repo *string
		if err := rows.Scan(&repo, &r.Duration); err != nil {
			return nil, err
		}
		if repo != nil {
			r.Repository = *repo
		} else {
			r.Repository = "unknown"
		}
		results = append(results, r)
	}
	return results, nil
}

// GetPoolMetrics calculates total and used slots per pool
func (c *Client) GetPoolMetrics(ctx context.Context) ([]PoolMetric, error) {
	sql := `
		SELECT
			sp.pool,
			sp.slots,
			COALESCE(SUM(ti.pool_slots), 0)
		FROM slot_pool sp
		LEFT JOIN task_instance ti ON sp.pool = ti.pool AND ti.state IN ('running', 'queued')
		GROUP BY sp.pool, sp.slots
	`
	rows, err := c.db.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var results []PoolMetric
	for rows.Next() {
		var r PoolMetric
		var pool *string
		if err := rows.Scan(&pool, &r.TotalSlots, &r.UsedSlots); err != nil {
			return nil, err
		}
		if pool != nil {
			r.Pool = *pool
		} else {
			r.Pool = "unknown"
		}
		results = append(results, r)
	}
	return results, nil
}

// GetImportErrors counts the import errors per repository
func (c *Client) GetImportErrors(ctx context.Context) ([]MetricCount, error) {
	sql := `
		SELECT 
			SUBSTRING(filename FROM '/opt/airflow/dags/([^/]+)/') as repository,
			COUNT(*)
		FROM import_error
		GROUP BY 1
	`
	rows, err := c.db.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var results []MetricCount
	for rows.Next() {
		var r MetricCount
		var repo *string
		if err := rows.Scan(&repo, &r.Count); err != nil {
			return nil, err
		}
		if repo != nil {
			r.Repository = *repo
		} else {
			r.Repository = "unknown"
		}
		r.Label = "import_error"
		results = append(results, r)
	}
	return results, nil
}

// GetZombieTasks counts potentially zombied tasks per repository
func (c *Client) GetZombieTasks(ctx context.Context) ([]MetricCount, error) {
	// A basic heuristic for Airflow 3 zombie tasks: running but no heartbeat in the last 5 minutes
	sql := `
		SELECT 
			SUBSTRING(d.fileloc FROM '/opt/airflow/dags/([^/]+)/') as repository,
			COUNT(*) 
		FROM task_instance ti
		JOIN dag d ON ti.dag_id = d.dag_id
		WHERE ti.state = 'running' 
		  AND (
			ti.last_heartbeat_at < NOW() - INTERVAL '5 minutes' 
			OR (ti.last_heartbeat_at IS NULL AND ti.start_date < NOW() - INTERVAL '5 minutes')
		  )
		GROUP BY 1
	`
	rows, err := c.db.Query(ctx, sql)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var results []MetricCount
	for rows.Next() {
		var r MetricCount
		var repo *string
		if err := rows.Scan(&repo, &r.Count); err != nil {
			return nil, err
		}
		if repo != nil {
			r.Repository = *repo
		} else {
			r.Repository = "unknown"
		}
		r.Label = "zombie"
		results = append(results, r)
	}
	return results, nil
}
