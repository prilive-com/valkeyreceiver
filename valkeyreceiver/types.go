package valkeyreceiver

import (
	"context"
	"log/slog"
	"time"
)

// Consumer defines the interface for consuming messages from Valkey queues
type Consumer interface {
	// Subscribe registers a handler for a specific queue
	Subscribe(queue string, handler MessageHandler) error
	
	// Start begins consuming messages (blocking operation)
	Start(ctx context.Context) error
	
	// Stop gracefully stops the consumer
	Stop() error
	
	// Close releases all resources
	Close() error
	
	// Health returns the current health status
	Health() HealthStatus
	
	// GetQueueSize returns the number of messages in a queue
	GetQueueSize(ctx context.Context, queue string) (int64, error)
}

// MessageHandler defines the interface for processing received messages
type MessageHandler interface {
	Handle(ctx context.Context, message Message) error
}

// MessageHandlerFunc is a function adapter for MessageHandler
type MessageHandlerFunc func(ctx context.Context, message Message) error

func (f MessageHandlerFunc) Handle(ctx context.Context, message Message) error {
	return f(ctx, message)
}

// Message represents a message received from a Valkey queue
type Message struct {
	Queue     string            `json:"queue"`
	Value     []byte            `json:"value"`
	Headers   map[string]string `json:"headers,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
	MessageID string            `json:"message_id"`
	TTL       time.Duration     `json:"ttl,omitempty"`
	Retries   int               `json:"retries,omitempty"`
}

// MessageEnvelope represents the envelope structure for messages in queues
type MessageEnvelope struct {
	ID        string            `json:"id"`
	Timestamp time.Time         `json:"timestamp"`
	TTL       time.Duration     `json:"ttl,omitempty"`
	Retries   int               `json:"retries"`
	Headers   map[string]string `json:"headers,omitempty"`
	Payload   interface{}       `json:"payload"`
}


// HealthStatus represents the consumer health information
type HealthStatus struct {
	Status            string        `json:"status"`            // healthy, degraded, unhealthy
	MessagesProcessed int64         `json:"messages_processed"`
	ErrorCount        int64         `json:"error_count"`
	LastMessageTime   time.Time     `json:"last_message_time"`
	Uptime            time.Duration `json:"uptime"`
	ConnectionState   string        `json:"connection_state"`  // connected, disconnected, reconnecting
	CircuitBreaker    string        `json:"circuit_breaker"`   // closed, half-open, open
	QueueSizes        map[string]int64 `json:"queue_sizes,omitempty"`
}

// ConsumerMetrics holds performance metrics for the consumer
type ConsumerMetrics struct {
	MessagesReceived    int64         `json:"messages_received"`
	MessagesProcessed   int64         `json:"messages_processed"`
	ProcessingErrors    int64         `json:"processing_errors"`
	AvgProcessingTime   time.Duration `json:"avg_processing_time"`
	LastProcessingTime  time.Duration `json:"last_processing_time"`
	CircuitBreakerState string        `json:"circuit_breaker_state"`
	ConnectionErrors    int64         `json:"connection_errors"`
	ReconnectAttempts   int64         `json:"reconnect_attempts"`
}

// ConsumerOptions holds optional configuration for the consumer
type ConsumerOptions struct {
	Logger *slog.Logger
	
	// Error handling
	ErrorHandler func(error)
	
	// Success callback
	SuccessHandler func(Message)
	
	// Metrics callback
	MetricsHandler func(ConsumerMetrics)
	
	// Queue rebalance callback
	RebalanceHandler func(queue string)
	
	// Retry configuration
	RetryAttempts int
	RetryBackoff  time.Duration
	
	// Processing configuration
	MaxConcurrentMessages int
	ProcessingTimeout      time.Duration
	
	// Dead letter queue
	DeadLetterQueue string
	MaxRetries      int
}

// QueueSubscription represents a queue subscription
type QueueSubscription struct {
	Queue   string
	Handler MessageHandler
}

// ConnectionState represents the connection state
type ConnectionState int

const (
	StateDisconnected ConnectionState = iota
	StateConnecting
	StateConnected
	StateReconnecting
)

func (s ConnectionState) String() string {
	switch s {
	case StateDisconnected:
		return "disconnected"
	case StateConnecting:
		return "connecting"
	case StateConnected:
		return "connected"
	case StateReconnecting:
		return "reconnecting"
	default:
		return "unknown"
	}
}