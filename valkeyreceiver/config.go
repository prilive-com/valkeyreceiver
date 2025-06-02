package valkeyreceiver

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds the configuration for the Valkey receiver
type Config struct {
	// Connection settings
	Address     string        `json:"address"`
	Username    string        `json:"username,omitempty"`
	Password    string        `json:"password,omitempty"`
	Database    int           `json:"database"`
	DialTimeout time.Duration `json:"dial_timeout"`
	ReadTimeout time.Duration `json:"read_timeout"`
	WriteTimeout time.Duration `json:"write_timeout"`

	// Consumer settings
	DefaultQueues        []string      `json:"default_queues"`
	ConsumerGroup        string        `json:"consumer_group"`
	BlockingTimeout      time.Duration `json:"blocking_timeout"`
	MaxConcurrentMessages int          `json:"max_concurrent_messages"`
	ProcessingTimeout    time.Duration `json:"processing_timeout"`

	// Connection pool settings
	PoolSize        int           `json:"pool_size"`
	MinIdleConns    int           `json:"min_idle_conns"`
	MaxIdleTime     time.Duration `json:"max_idle_time"`
	ConnMaxLifetime time.Duration `json:"conn_max_lifetime"`

	// Retry settings
	MaxRetries   int           `json:"max_retries"`
	RetryDelay   time.Duration `json:"retry_delay"`
	RetryBackoff float64       `json:"retry_backoff"`

	// Circuit breaker settings
	BreakerMaxRequests uint32        `json:"breaker_max_requests"`
	BreakerInterval    time.Duration `json:"breaker_interval"`
	BreakerTimeout     time.Duration `json:"breaker_timeout"`

	// Rate limiting settings
	RateLimitRequests int `json:"rate_limit_requests"`
	RateLimitBurst    int `json:"rate_limit_burst"`

	// TLS settings
	TLSEnabled    bool   `json:"tls_enabled"`
	TLSCertFile   string `json:"tls_cert_file,omitempty"`
	TLSKeyFile    string `json:"tls_key_file,omitempty"`
	TLSCAFile     string `json:"tls_ca_file,omitempty"`
	TLSSkipVerify bool   `json:"tls_skip_verify"`

	// Dead letter queue settings
	DeadLetterQueue string `json:"dead_letter_queue,omitempty"`
	
	// Startup drain settings
	ProcessExistingMessages bool          `json:"process_existing_messages"`
	StartupDrainTimeout     time.Duration `json:"startup_drain_timeout"`
	
	// Logging
	LogLevel string `json:"log_level"`
}

// LoadConfig loads configuration from environment variables
func LoadConfig() (*Config, error) {
	config := &Config{
		// Connection defaults
		Address:     getEnvOrDefault("VALKEY_RECEIVER_ADDRESS", "localhost:6379"),
		Username:    os.Getenv("VALKEY_RECEIVER_USERNAME"),
		Password:    os.Getenv("VALKEY_RECEIVER_PASSWORD"),
		Database:    getEnvAsIntOrDefault("VALKEY_RECEIVER_DATABASE", 0),
		DialTimeout: getEnvAsDurationOrDefault("VALKEY_RECEIVER_DIAL_TIMEOUT", 5*time.Second),
		ReadTimeout: getEnvAsDurationOrDefault("VALKEY_RECEIVER_READ_TIMEOUT", 3*time.Second),
		WriteTimeout: getEnvAsDurationOrDefault("VALKEY_RECEIVER_WRITE_TIMEOUT", 3*time.Second),

		// Consumer defaults
		DefaultQueues:         getEnvAsSliceOrDefault("VALKEY_RECEIVER_DEFAULT_QUEUES", []string{"user-registrations"}),
		ConsumerGroup:         getEnvOrDefault("VALKEY_RECEIVER_CONSUMER_GROUP", "default-group"),
		BlockingTimeout:       getEnvAsDurationOrDefault("VALKEY_RECEIVER_BLOCKING_TIMEOUT", 1*time.Second),
		MaxConcurrentMessages: getEnvAsIntOrDefault("VALKEY_RECEIVER_MAX_CONCURRENT_MESSAGES", 10),
		ProcessingTimeout:     getEnvAsDurationOrDefault("VALKEY_RECEIVER_PROCESSING_TIMEOUT", 30*time.Second),

		// Connection pool defaults
		PoolSize:        getEnvAsIntOrDefault("VALKEY_RECEIVER_POOL_SIZE", 10),
		MinIdleConns:    getEnvAsIntOrDefault("VALKEY_RECEIVER_MIN_IDLE_CONNS", 2),
		MaxIdleTime:     getEnvAsDurationOrDefault("VALKEY_RECEIVER_MAX_IDLE_TIME", 5*time.Minute),
		ConnMaxLifetime: getEnvAsDurationOrDefault("VALKEY_RECEIVER_CONN_MAX_LIFETIME", 1*time.Hour),

		// Retry defaults
		MaxRetries:   getEnvAsIntOrDefault("VALKEY_RECEIVER_MAX_RETRIES", 3),
		RetryDelay:   getEnvAsDurationOrDefault("VALKEY_RECEIVER_RETRY_DELAY", 1*time.Second),
		RetryBackoff: getEnvAsFloatOrDefault("VALKEY_RECEIVER_RETRY_BACKOFF", 2.0),

		// Circuit breaker defaults
		BreakerMaxRequests: uint32(getEnvAsIntOrDefault("VALKEY_RECEIVER_BREAKER_MAX_REQUESTS", 5)),
		BreakerInterval:    getEnvAsDurationOrDefault("VALKEY_RECEIVER_BREAKER_INTERVAL", 2*time.Minute),
		BreakerTimeout:     getEnvAsDurationOrDefault("VALKEY_RECEIVER_BREAKER_TIMEOUT", 60*time.Second),

		// Rate limiting defaults
		RateLimitRequests: getEnvAsIntOrDefault("VALKEY_RECEIVER_RATE_LIMIT_REQUESTS", 1000),
		RateLimitBurst:    getEnvAsIntOrDefault("VALKEY_RECEIVER_RATE_LIMIT_BURST", 2000),

		// TLS defaults
		TLSEnabled:    getEnvAsBoolOrDefault("VALKEY_RECEIVER_TLS_ENABLED", false),
		TLSCertFile:   os.Getenv("VALKEY_RECEIVER_TLS_CERT_FILE"),
		TLSKeyFile:    os.Getenv("VALKEY_RECEIVER_TLS_KEY_FILE"),
		TLSCAFile:     os.Getenv("VALKEY_RECEIVER_TLS_CA_FILE"),
		TLSSkipVerify: getEnvAsBoolOrDefault("VALKEY_RECEIVER_TLS_SKIP_VERIFY", false),

		// Dead letter queue
		DeadLetterQueue: os.Getenv("VALKEY_RECEIVER_DEAD_LETTER_QUEUE"),

		// Startup drain defaults
		ProcessExistingMessages: getEnvAsBoolOrDefault("VALKEY_RECEIVER_PROCESS_EXISTING_MESSAGES", true),
		StartupDrainTimeout:     getEnvAsDurationOrDefault("VALKEY_RECEIVER_STARTUP_DRAIN_TIMEOUT", 30*time.Second),

		// Logging defaults
		LogLevel: getEnvOrDefault("VALKEY_RECEIVER_LOG_LEVEL", "INFO"),
	}

	if err := validateConfig(config); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	return config, nil
}

// validateConfig validates the configuration
func validateConfig(config *Config) error {
	if config.Address == "" {
		return fmt.Errorf("VALKEY_RECEIVER_ADDRESS is required")
	}

	if config.Database < 0 || config.Database > 15 {
		return fmt.Errorf("VALKEY_RECEIVER_DATABASE must be between 0 and 15")
	}

	if config.PoolSize <= 0 {
		return fmt.Errorf("VALKEY_RECEIVER_POOL_SIZE must be greater than 0")
	}

	if config.MaxConcurrentMessages <= 0 {
		return fmt.Errorf("VALKEY_RECEIVER_MAX_CONCURRENT_MESSAGES must be greater than 0")
	}

	if config.BlockingTimeout < 0 {
		return fmt.Errorf("VALKEY_RECEIVER_BLOCKING_TIMEOUT must be non-negative")
	}

	if config.ProcessingTimeout <= 0 {
		return fmt.Errorf("VALKEY_RECEIVER_PROCESSING_TIMEOUT must be greater than 0")
	}

	if len(config.DefaultQueues) == 0 {
		return fmt.Errorf("VALKEY_RECEIVER_DEFAULT_QUEUES must not be empty")
	}

	if config.StartupDrainTimeout <= 0 {
		return fmt.Errorf("VALKEY_RECEIVER_STARTUP_DRAIN_TIMEOUT must be greater than 0")
	}

	validLogLevels := map[string]bool{
		"DEBUG": true, "INFO": true, "WARN": true, "ERROR": true,
	}
	if !validLogLevels[strings.ToUpper(config.LogLevel)] {
		return fmt.Errorf("VALKEY_RECEIVER_LOG_LEVEL must be one of: DEBUG, INFO, WARN, ERROR")
	}

	return nil
}

// Helper functions for environment variable parsing
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvAsIntOrDefault(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}

func getEnvAsBoolOrDefault(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if boolValue, err := strconv.ParseBool(value); err == nil {
			return boolValue
		}
	}
	return defaultValue
}

func getEnvAsDurationOrDefault(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		if duration, err := time.ParseDuration(value); err == nil {
			return duration
		}
	}
	return defaultValue
}

func getEnvAsFloatOrDefault(key string, defaultValue float64) float64 {
	if value := os.Getenv(key); value != "" {
		if floatValue, err := strconv.ParseFloat(value, 64); err == nil {
			return floatValue
		}
	}
	return defaultValue
}

func getEnvAsSliceOrDefault(key string, defaultValue []string) []string {
	if value := os.Getenv(key); value != "" {
		return strings.Split(value, ",")
	}
	return defaultValue
}