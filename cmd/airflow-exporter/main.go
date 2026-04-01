package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/everton/airflow-exporter/internal/airflow"
	"github.com/everton/airflow-exporter/internal/collector"
	"github.com/everton/airflow-exporter/internal/config"
	"github.com/everton/airflow-exporter/internal/db"
	"github.com/everton/airflow-exporter/internal/telemetry"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

var rootCmd = &cobra.Command{
	Use:   "airflow-exporter",
	Short: "Prometheus/OpenTelemetry exporter for Airflow 3",
}

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the exporter server",
	Run: func(cmd *cobra.Command, args []string) {
		runServer()
	},
}

func init() {
	rootCmd.AddCommand(serveCmd)
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func runServer() {
	// 1. Config
	cfg, err := config.Load()
	if err != nil {
		fmt.Printf("Failed to load config: %v\n", err)
		os.Exit(1)
	}

	// 2. Logger
	logger := telemetry.InitLogger(cfg.Log.Level)
	defer logger.Sync()

	// Obfuscate sensitive fields before logging
	safeCfg := *cfg
	safeCfg.Database.Password = "****"
	if safeCfg.Database.ConnectionString != "" {
		// Optionally mask any password within connection string
		safeCfg.Database.ConnectionString = "***redacted***"
	}
	logger.Info("Starting Airflow Exporter", zap.Any("config", safeCfg))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 3. Telemetry (OTel)
	shutdownOTel, err := telemetry.InitOTel(ctx, "airflow-exporter", cfg.OTel.Endpoint)
	if err != nil {
		logger.Fatal("Failed to init OTel", zap.Error(err))
	}
	// Check OTLP connectivity early
	if err := telemetry.CheckOTel(ctx, 5*time.Second); err != nil {
		logger.Fatal("OTel connectivity check failed", zap.Error(err))
	}
	logger.Info("OTLP connectivity check succeeded", zap.String("endpoint", cfg.OTel.Endpoint))
	defer func() {
		if err := shutdownOTel(ctx); err != nil {
			logger.Error("Failed to shutdown OTel", zap.Error(err))
		}
	}()

	// 4. Database
	dbPool, err := db.Connect(ctx, cfg.Database.Host, cfg.Database.Port, cfg.Database.Name, cfg.Database.User, cfg.Database.Password)
	if err != nil {
		// Just log error but don't crash yet, maybe DB comes up later?
		// Actually for an exporter it's better to crash or handle retry.
		// We'll Log Fatal as Golden Path suggests "Fail Fast".
		logger.Fatal("Failed to connect to DB", zap.Error(err))
	}
	defer dbPool.Close()

	// Check DB connectivity early: SELECT 1 with short timeout
	{
		chkCtx, chkCancel := context.WithTimeout(ctx, 5*time.Second)
		defer chkCancel()
		row := dbPool.QueryRow(chkCtx, "SELECT 1")
		var one int
		if err := row.Scan(&one); err != nil || one != 1 {
			logger.Fatal("Database connectivity check failed", zap.Error(err))
		}
		logger.Info("Database connectivity check succeeded",
			zap.String("host", cfg.Database.Host),
			zap.Int("port", cfg.Database.Port),
			zap.String("db", cfg.Database.Name))
	}

	// 5. Collector
	airflowClient := airflow.NewClient(dbPool)
	meter := otel.GetMeterProvider().Meter("airflow-exporter")
	col, err := collector.New(airflowClient, logger, meter)
	if err != nil {
		logger.Fatal("Failed to create collector", zap.Error(err))
	}

	// Start Collector in background
	interval, err := time.ParseDuration(cfg.Scrape.Interval)
	if err != nil {
		logger.Error("Failed to parse scrape interval, using default 30s", zap.String("interval", cfg.Scrape.Interval), zap.Error(err))
		interval = 30 * time.Second
	}
	go col.Start(ctx, interval)

	// 6. Web Server (Echo)
	e := echo.New()
	e.HideBanner = true
	e.Use(middleware.Recover())
	e.Use(middleware.RequestID())
	e.Use(middleware.LoggerWithConfig(middleware.LoggerConfig{
		Format: `{"time":"${time_rfc3339}","remote_ip":"${remote_ip}","host":"${host}","method":"${method}","uri":"${uri}","status":${status},"error":"${error}"}` + "\n",
	}))

	e.GET("/health", func(c echo.Context) error {
		return c.JSON(http.StatusOK, map[string]string{"status": "ok"})
	})

	// // Force an immediate scrape
	// e.POST("/scrape", func(c echo.Context) error {
	// 	col.Scrape(ctx)
	// 	return c.JSON(http.StatusOK, map[string]string{"status": "scrape_triggered"})
	// })

	// 7. Start Server with Graceful Shutdown
	go func() {
		if err := e.Start(":" + cfg.Server.Port); err != nil && err != http.ErrServerClosed {
			logger.Fatal("shutting down check the server", zap.Error(err))
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	logger.Info("Shutting down...")
	cancel() // Stop collector

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := e.Shutdown(shutdownCtx); err != nil {
		logger.Fatal("Server forced to shutdown", zap.Error(err))
	}
	logger.Info("Server exited")
}
