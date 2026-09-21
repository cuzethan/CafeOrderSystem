package config

import (
	"fmt"
	"os"
)

// Config holds runtime settings loaded from the environment.
type Config struct {
	HTTPAddr    string
	DatabaseURL string
	RabbitMQURL string
}

// Load reads configuration from environment variables with sensible local defaults.
func Load() (Config, error) {
	cfg := Config{
		HTTPAddr:    getenv("HTTP_ADDR", ":8080"),
		DatabaseURL: os.Getenv("DATABASE_URL"),
		RabbitMQURL: getenv("RABBITMQ_URL", "amqp://guest:guest@localhost:5672/"),
	}
	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL is required")
	}
	return cfg, nil
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
