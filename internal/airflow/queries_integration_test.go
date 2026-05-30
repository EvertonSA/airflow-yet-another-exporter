package airflow

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// To run this integration test, set the environment variable:
// export TEST_DATABASE_URL="postgres://airflow:airflow_password@localhost:5432/airflow?sslmode=disable"
func TestQueriesIntegration(t *testing.T) {
	connStr := os.Getenv("TEST_DATABASE_URL")
	if connStr == "" {
		t.Skip("Skipping integration test: TEST_DATABASE_URL environment variable is not set")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, connStr)
	if err != nil {
		t.Fatalf("failed to connect to test database: %v", err)
	}
	defer pool.Close()

	// Ensure connection works
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("failed to ping test database: %v", err)
	}

	// 1. Setup mock database tables and insert mock data
	cleanup := setupTestSchemaAndData(t, ctx, pool)
	defer cleanup()

	// 2. Initialize the Airflow Client under test
	client := NewClient(pool)

	// 3. Run and assert each query method

	t.Run("GetDagRunStates", func(t *testing.T) {
		states, err := client.GetDagRunStates(ctx)
		if err != nil {
			t.Fatalf("GetDagRunStates failed: %v", err)
		}
		// Expecting:
		// - repo-alpha: success (1), failed (1)
		// - repo-beta: running (1)
		expected := map[string]map[string]int{
			"repo-alpha": {"success": 1, "failed": 1},
			"repo-beta":  {"running": 1},
		}

		counts := make(map[string]map[string]int)
		for _, s := range states {
			if _, ok := counts[s.Repository]; !ok {
				counts[s.Repository] = make(map[string]int)
			}
			counts[s.Repository][s.Label] = s.Count
		}

		for repo, stateMap := range expected {
			for state, val := range stateMap {
				if counts[repo][state] != val {
					t.Errorf("expected %s in %s to be %d, got %d", state, repo, val, counts[repo][state])
				}
			}
		}
	})

	t.Run("GetTaskInstanceStates", func(t *testing.T) {
		states, err := client.GetTaskInstanceStates(ctx)
		if err != nil {
			t.Fatalf("GetTaskInstanceStates failed: %v", err)
		}
		// Expecting:
		// - repo-alpha: success (1), failed (1)
		// - repo-beta: running (1)
		expected := map[string]map[string]int{
			"repo-alpha": {"success": 1, "failed": 1},
			"repo-beta":  {"running": 1},
		}

		counts := make(map[string]map[string]int)
		for _, s := range states {
			if _, ok := counts[s.Repository]; !ok {
				counts[s.Repository] = make(map[string]int)
			}
			counts[s.Repository][s.Label] = s.Count
		}

		for repo, stateMap := range expected {
			for state, val := range stateMap {
				if counts[repo][state] != val {
					t.Errorf("expected task instance state %s in %s to be %d, got %d", state, repo, val, counts[repo][state])
				}
			}
		}
	})

	t.Run("GetOperatorFailures", func(t *testing.T) {
		failures, err := client.GetOperatorFailures(ctx)
		if err != nil {
			t.Fatalf("GetOperatorFailures failed: %v", err)
		}
		// Expecting repo-alpha: BashOperator (1)
		found := false
		for _, f := range failures {
			if f.Repository == "repo-alpha" && f.Label == "BashOperator" {
				if f.Count != 1 {
					t.Errorf("expected 1 BashOperator failure in repo-alpha, got %d", f.Count)
				}
				found = true
			}
		}
		if !found {
			t.Error("expected to find operator failure for BashOperator in repo-alpha")
		}
	})

	t.Run("GetDagRunDurations", func(t *testing.T) {
		durations, err := client.GetDagRunDurations(ctx)
		if err != nil {
			t.Fatalf("GetDagRunDurations failed: %v", err)
		}
		// Expecting repo-alpha: avg of 120 and 60 = 90 seconds
		found := false
		for _, d := range durations {
			if d.Repository == "repo-alpha" {
				if d.Duration < 89.0 || d.Duration > 91.0 {
					t.Errorf("expected average dag run duration in repo-alpha to be ~90.0, got %f", d.Duration)
				}
				found = true
			}
		}
		if !found {
			t.Error("expected to find dag run duration for repo-alpha")
		}
	})

	t.Run("GetTaskDurations", func(t *testing.T) {
		durations, err := client.GetTaskDurations(ctx)
		if err != nil {
			t.Fatalf("GetTaskDurations failed: %v", err)
		}
		// Expecting repo-alpha: avg of 30 and 5 = 17.5 seconds
		found := false
		for _, d := range durations {
			if d.Repository == "repo-alpha" {
				if d.Duration < 17.4 || d.Duration > 17.6 {
					t.Errorf("expected average task duration in repo-alpha to be ~17.5, got %f", d.Duration)
				}
				found = true
			}
		}
		if !found {
			t.Error("expected to find task duration for repo-alpha")
		}
	})

	t.Run("GetTaskQueueWait", func(t *testing.T) {
		waits, err := client.GetTaskQueueWait(ctx)
		if err != nil {
			t.Fatalf("GetTaskQueueWait failed: %v", err)
		}
		// Expecting repo-alpha: avg of 10 and 10 = 10.0 seconds
		found := false
		for _, w := range waits {
			if w.Repository == "repo-alpha" {
				if w.Duration < 9.9 || w.Duration > 10.1 {
					t.Errorf("expected average task queue wait in repo-alpha to be ~10.0, got %f", w.Duration)
				}
				found = true
			}
		}
		if !found {
			t.Error("expected to find task queue wait for repo-alpha")
		}
	})

	t.Run("GetDagParseDurations", func(t *testing.T) {
		durations, err := client.GetDagParseDurations(ctx)
		if err != nil {
			t.Fatalf("GetDagParseDurations failed: %v", err)
		}
		// Expecting:
		// repo-alpha: 1.5
		// repo-beta: 3.2
		expected := map[string]float64{
			"repo-alpha": 1.5,
			"repo-beta":  3.2,
		}
		for _, d := range durations {
			if exp, ok := expected[d.Repository]; ok {
				if d.Duration < exp-0.1 || d.Duration > exp+0.1 {
					t.Errorf("expected parse duration for %s to be ~%f, got %f", d.Repository, exp, d.Duration)
				}
			}
		}
	})

	t.Run("GetPoolMetrics", func(t *testing.T) {
		metrics, err := client.GetPoolMetrics(ctx)
		if err != nil {
			t.Fatalf("GetPoolMetrics failed: %v", err)
		}
		// Expecting: test_pool: TotalSlots (10), UsedSlots (2 - from running task_c with pool_slots = 2)
		found := false
		for _, m := range metrics {
			if m.Pool == "test_pool" {
				if m.TotalSlots != 10 {
					t.Errorf("expected total slots for test_pool to be 10, got %d", m.TotalSlots)
				}
				if m.UsedSlots != 2 {
					t.Errorf("expected used slots for test_pool to be 2, got %d", m.UsedSlots)
				}
				found = true
			}
		}
		if !found {
			t.Error("expected to find pool metrics for test_pool")
		}
	})

	t.Run("GetImportErrors", func(t *testing.T) {
		errors, err := client.GetImportErrors(ctx)
		if err != nil {
			t.Fatalf("GetImportErrors failed: %v", err)
		}
		// Expecting repo-alpha: count 1
		found := false
		for _, e := range errors {
			if e.Repository == "repo-alpha" {
				if e.Count != 1 {
					t.Errorf("expected 1 import error in repo-alpha, got %d", e.Count)
				}
				found = true
			}
		}
		if !found {
			t.Error("expected to find import errors for repo-alpha")
		}
	})

	t.Run("GetZombieTasks", func(t *testing.T) {
		zombies, err := client.GetZombieTasks(ctx)
		if err != nil {
			t.Fatalf("GetZombieTasks failed: %v", err)
		}
		// Expecting repo-beta: count 1 (task_c has state running and last heartbeat older than 5 minutes)
		found := false
		for _, z := range zombies {
			if z.Repository == "repo-beta" {
				if z.Count != 1 {
					t.Errorf("expected 1 zombie task in repo-beta, got %d", z.Count)
				}
				found = true
			}
		}
		if !found {
			t.Error("expected to find zombie tasks for repo-beta")
		}
	})
}

func setupTestSchemaAndData(t *testing.T, ctx context.Context, pool *pgxpool.Pool) func() {
	// Drop existing tables to ensure clean state
	tables := []string{"import_error", "task_instance", "dag_run", "dag", "slot_pool"}
	for _, table := range tables {
		_, err := pool.Exec(ctx, "DROP TABLE IF EXISTS "+table+" CASCADE")
		if err != nil {
			t.Fatalf("failed to drop table %s: %v", table, err)
		}
	}

	// Create tables matching schema expected by queries
	queries := []string{
		`CREATE TABLE slot_pool (
			pool VARCHAR(255) PRIMARY KEY,
			slots INTEGER NOT NULL
		)`,
		`CREATE TABLE dag (
			dag_id VARCHAR(255) PRIMARY KEY,
			fileloc VARCHAR(1024) NOT NULL,
			last_parse_duration DOUBLE PRECISION
		)`,
		`CREATE TABLE dag_run (
			id SERIAL PRIMARY KEY,
			run_id VARCHAR(255) UNIQUE NOT NULL,
			dag_id VARCHAR(255) NOT NULL REFERENCES dag(dag_id),
			state VARCHAR(50) NOT NULL,
			start_date TIMESTAMP,
			end_date TIMESTAMP
		)`,
		`CREATE TABLE task_instance (
			task_id VARCHAR(255) NOT NULL,
			dag_id VARCHAR(255) NOT NULL REFERENCES dag(dag_id),
			run_id VARCHAR(255) NOT NULL,
			state VARCHAR(50) NOT NULL,
			operator VARCHAR(255) NOT NULL,
			pool VARCHAR(255) NOT NULL REFERENCES slot_pool(pool),
			pool_slots INTEGER DEFAULT 1,
			start_date TIMESTAMP,
			end_date TIMESTAMP,
			queued_dttm TIMESTAMP,
			last_heartbeat_at TIMESTAMP,
			PRIMARY KEY (dag_id, task_id, run_id)
		)`,
		`CREATE TABLE import_error (
			id SERIAL PRIMARY KEY,
			filename VARCHAR(1024) NOT NULL,
			stacktrace TEXT
		)`,
	}

	for _, q := range queries {
		_, err := pool.Exec(ctx, q)
		if err != nil {
			t.Fatalf("failed to create table: query=%q, err=%v", q, err)
		}
	}

	// Insert mock data
	now := time.Now()

	// 1. Slot pools
	_, err := pool.Exec(ctx, "INSERT INTO slot_pool (pool, slots) VALUES ($1, $2)", "test_pool", 10)
	if err != nil {
		t.Fatalf("failed to seed slot_pool: %v", err)
	}

	// 2. DAGs
	_, err = pool.Exec(ctx, "INSERT INTO dag (dag_id, fileloc, last_parse_duration) VALUES ($1, $2, $3)",
		"dag_one", "/opt/airflow/dags/repo-alpha/dag_one.py", 1.5)
	if err != nil {
		t.Fatalf("failed to seed dag 1: %v", err)
	}
	_, err = pool.Exec(ctx, "INSERT INTO dag (dag_id, fileloc, last_parse_duration) VALUES ($1, $2, $3)",
		"dag_two", "/opt/airflow/dags/repo-beta/dag_two.py", 3.2)
	if err != nil {
		t.Fatalf("failed to seed dag 2: %v", err)
	}

	// 3. DAG Runs
	// repo-alpha runs
	_, err = pool.Exec(ctx, "INSERT INTO dag_run (run_id, dag_id, state, start_date, end_date) VALUES ($1, $2, $3, $4, $5)",
		"run_alpha_success", "dag_one", "success", now.Add(-120*time.Second), now)
	if err != nil {
		t.Fatalf("failed to seed dag_run 1: %v", err)
	}
	_, err = pool.Exec(ctx, "INSERT INTO dag_run (run_id, dag_id, state, start_date, end_date) VALUES ($1, $2, $3, $4, $5)",
		"run_alpha_failed", "dag_one", "failed", now.Add(-60*time.Second), now)
	if err != nil {
		t.Fatalf("failed to seed dag_run 2: %v", err)
	}
	// repo-beta run (running)
	_, err = pool.Exec(ctx, "INSERT INTO dag_run (run_id, dag_id, state, start_date, end_date) VALUES ($1, $2, $3, $4, $5)",
		"run_beta_running", "dag_two", "running", now.Add(-10*time.Minute), nil)
	if err != nil {
		t.Fatalf("failed to seed dag_run 3: %v", err)
	}

	// 4. Task Instances
	// task_a: success, python operator, duration 30s, queue wait 10s
	_, err = pool.Exec(ctx, `INSERT INTO task_instance 
		(task_id, dag_id, run_id, state, operator, pool, pool_slots, start_date, end_date, queued_dttm, last_heartbeat_at) 
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		"task_a", "dag_one", "run_alpha_success", "success", "PythonOperator", "test_pool", 1,
		now.Add(-30*time.Second), now, now.Add(-40*time.Second), now)
	if err != nil {
		t.Fatalf("failed to seed task_instance a: %v", err)
	}

	// task_b: failed, bash operator, duration 5s, queue wait 10s
	_, err = pool.Exec(ctx, `INSERT INTO task_instance 
		(task_id, dag_id, run_id, state, operator, pool, pool_slots, start_date, end_date, queued_dttm, last_heartbeat_at) 
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		"task_b", "dag_one", "run_alpha_failed", "failed", "BashOperator", "test_pool", 1,
		now.Add(-5*time.Second), now, now.Add(-15*time.Second), now)
	if err != nil {
		t.Fatalf("failed to seed task_instance b: %v", err)
	}

	// task_c: running, custom operator, pool_slots 2, heartbeat 10 minutes ago -> ZOMBIE
	_, err = pool.Exec(ctx, `INSERT INTO task_instance 
		(task_id, dag_id, run_id, state, operator, pool, pool_slots, start_date, end_date, queued_dttm, last_heartbeat_at) 
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)`,
		"task_c", "dag_two", "run_beta_running", "running", "CustomOperator", "test_pool", 2,
		now.Add(-10*time.Minute), nil, now.Add(-11*time.Minute), now.Add(-10*time.Minute))
	if err != nil {
		t.Fatalf("failed to seed task_instance c: %v", err)
	}

	// 5. Import Errors
	_, err = pool.Exec(ctx, "INSERT INTO import_error (filename, stacktrace) VALUES ($1, $2)",
		"/opt/airflow/dags/repo-alpha/broken.py", "Traceback (most recent call last): ...")
	if err != nil {
		t.Fatalf("failed to seed import_error: %v", err)
	}

	// Return cleanup function
	return func() {
		for _, table := range tables {
			pool.Exec(ctx, "DROP TABLE IF EXISTS "+table+" CASCADE")
		}
	}
}
