package config

import (
	"fmt"
	"os"
	"strings"
)

func HTTPAddr() string {
	if addr := strings.TrimSpace(os.Getenv("HTTP_ADDR")); addr != "" {
		return addr
	}
	return ":8080"
}

func DatabaseURL() (string, error) {
	url := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if url == "" {
		return "", fmt.Errorf("DATABASE_URL is required")
	}
	return url, nil
}

func APIKey() (string, error) {
	key := strings.TrimSpace(os.Getenv("API_KEY"))
	if key == "" {
		return "", fmt.Errorf("API_KEY is required")
	}
	return key, nil
}

func RedisAddr() string {
	if addr := strings.TrimSpace(os.Getenv("REDIS_ADDR")); addr != "" {
		return addr
	}
	return "localhost:6379"
}
