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
}

type ServerConfig struct {
	Port string `mapstructure:"port"`
}

type DatabaseConfig struct {
	ConnectionString string `mapstructure:"connection_string"`
}

type LogConfig struct {
	Level string `mapstructure:"level"`
}

type OTelConfig struct {
	Endpoint string `mapstructure:"endpoint"`
}

func Load() (*Config, error) {
	v := viper.New()

	// Defaults
	v.SetDefault("server.port", "8080")
	v.SetDefault("log.level", "info")
	v.SetDefault("otel.endpoint", "localhost:4317")
	v.SetDefault("database.connection_string", "") // Required for Viper to pick up env var for nested struct

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
