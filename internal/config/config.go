package config

import (
	"strings"

	"github.com/spf13/viper"
)

type Config struct {
	Server   ServerConfig   `mapstructure:"server"`
	Database DatabaseConfig `mapstructure:"database"`
	Log      LogConfig      `mapstructure:"log"`
	OTel     OTelConfig     `mapstructure:"otel"`
	Scrape   ScrapeConfig   `mapstructure:"scrape"`
}

type ServerConfig struct {
	Port string `mapstructure:"port"`
}

type DatabaseConfig struct {
	// Use individual parameters for DB configuration
	Host     string `mapstructure:"host"`
	Port     int    `mapstructure:"port"`
	Name     string `mapstructure:"name"`
	User     string `mapstructure:"user"`
	Password string `mapstructure:"password"`
	// Keep connection string optional for backward compatibility (ignored if individual params are used)
	ConnectionString string `mapstructure:"connection_string"`
}

type LogConfig struct {
	Level string `mapstructure:"level"`
}

type OTelConfig struct {
	Endpoint string `mapstructure:"endpoint"`
}

type ScrapeConfig struct {
	Interval string `mapstructure:"interval"`
}

func Load() (*Config, error) {
	v := viper.New()

	// Defaults
	v.SetDefault("server.port", "8080")
	v.SetDefault("log.level", "info")
	v.SetDefault("otel.endpoint", "127.0.0.1:4317")
	// Database defaults (parameter-by-parameter)
	v.SetDefault("database.host", "localhost")
	v.SetDefault("database.port", 5433)
	v.SetDefault("database.name", "airflow")
	v.SetDefault("database.user", "airflow")
	v.SetDefault("database.password", "airflow")
	v.SetDefault("scrape.interval", "30s")

	// Environment Variables
	v.SetEnvPrefix("AIRFLOW_EXPORTER")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Config File (Optional)
	v.SetConfigName("config")
	v.AddConfigPath(".")
	v.AddConfigPath("/etc/airflow-exporter/")

	_ = v.ReadInConfig() // Ignore error if config file not found

	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}
