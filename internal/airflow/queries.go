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
