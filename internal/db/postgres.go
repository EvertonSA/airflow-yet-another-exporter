package db

import (
	"context"
	"fmt"
	"net/url"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// BuildConnString constructs a Postgres connection string from individual parameters.
// Password and username are properly URL-encoded via url.UserPassword.
func BuildConnString(host string, port int, name, user, password string) string {
	u := &url.URL{
		Scheme:   "postgres",
		Host:     fmt.Sprintf("%s:%d", host, port),
		Path:     name,
		RawQuery: "sslmode=require",
		User:     url.UserPassword(user, password),
	}
	return u.String()
}

// Connect builds the DSN from parameters and connects using pgxpool.
func Connect(ctx context.Context, host string, port int, name, user, password string) (*pgxpool.Pool, error) {
	connString := BuildConnString(host, port, name, user, password)
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	config.MaxConns = 10
	config.MinConns = 2
	config.MaxConnLifetime = time.Hour
	config.MaxConnIdleTime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return pool, nil
}
