package valkeyreceiver

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/sony/gobreaker"
	"golang.org/x/time/rate"
)

// consumer implements the Consumer interface
type consumer struct {
	config       *Config
	client       *redis.Client
	logger       *slog.Logger
	circuitBreaker *gobreaker.CircuitBreaker
	rateLimiter    *rate.Limiter

	// Subscriptions and handlers
	subscriptions map[string]MessageHandler
	subMutex      sync.RWMutex

	// Control channels
	stopChan   chan struct{}
	stoppedChan chan struct{}
	isRunning  atomic.Bool
	startTime  time.Time

	// Metrics
	messagesReceived  atomic.Int64
	messagesProcessed atomic.Int64
	processingErrors  atomic.Int64
	connectionErrors  atomic.Int64
	reconnectAttempts atomic.Int64
	connectionState   atomic.Int32 // ConnectionState enum

	// Processing control
	processingWg sync.WaitGroup
	semaphore    chan struct{} // Semaphore for concurrent message processing

	// Options
	options *ConsumerOptions
}

// NewConsumer creates a new Valkey consumer with production-ready features
func NewConsumer(config *Config, options *ConsumerOptions) (Consumer, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	// Set default options if nil
	if options == nil {
		options = &ConsumerOptions{}
	}

	// Set default values for options
	if options.RetryAttempts == 0 {
		options.RetryAttempts = config.MaxRetries
	}
	if options.RetryBackoff == 0 {
		options.RetryBackoff = config.RetryDelay
	}
	if options.MaxConcurrentMessages == 0 {
		options.MaxConcurrentMessages = config.MaxConcurrentMessages
	}
	if options.ProcessingTimeout == 0 {
		options.ProcessingTimeout = config.ProcessingTimeout
	}
	if options.MaxRetries == 0 {
		options.MaxRetries = config.MaxRetries
	}

	// Create logger if not provided
	logger := options.Logger
	if logger == nil {
		var err error
		logger, err = NewLogger(ParseLogLevel(config.LogLevel), "")
		if err != nil {
			return nil, fmt.Errorf("failed to create logger: %w", err)
		}
	}

	// Create Redis client with production-ready settings
	redisOptions := &redis.Options{
		Addr:            config.Address,
		Username:        config.Username,
		Password:        config.Password,
		DB:              config.Database,
		DialTimeout:     config.DialTimeout,
		ReadTimeout:     config.ReadTimeout,
		WriteTimeout:    config.WriteTimeout,
		PoolSize:        config.PoolSize,
		MinIdleConns:    config.MinIdleConns,
		ConnMaxIdleTime: config.MaxIdleTime,
		ConnMaxLifetime: config.ConnMaxLifetime,
	}

	// Configure TLS if enabled
	if config.TLSEnabled {
		redisOptions.TLSConfig = &tls.Config{
			InsecureSkipVerify: config.TLSSkipVerify,
		}
	}

	client := redis.NewClient(redisOptions)

	// Create circuit breaker for resilience
	circuitBreakerSettings := gobreaker.Settings{
		Name:        "valkeyreceiver",
		MaxRequests: config.BreakerMaxRequests,
		Interval:    config.BreakerInterval,
		Timeout:     config.BreakerTimeout,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
			return counts.Requests >= 3 && failureRatio >= 0.6
		},
		OnStateChange: func(name string, from gobreaker.State, to gobreaker.State) {
			logger.Info("Circuit breaker state changed",
				"from", from.String(),
				"to", to.String(),
				"component", "valkeyreceiver")
		},
	}

	circuitBreaker := gobreaker.NewCircuitBreaker(circuitBreakerSettings)

	// Create rate limiter
	rateLimiter := rate.NewLimiter(
		rate.Limit(config.RateLimitRequests),
		config.RateLimitBurst,
	)

	c := &consumer{
		config:         config,
		client:         client,
		logger:         logger,
		circuitBreaker: circuitBreaker,
		rateLimiter:    rateLimiter,
		subscriptions:  make(map[string]MessageHandler),
		stopChan:       make(chan struct{}),
		stoppedChan:    make(chan struct{}),
		startTime:      time.Now(),
		semaphore:      make(chan struct{}, options.MaxConcurrentMessages),
		options:        options,
	}

	c.connectionState.Store(int32(StateDisconnected))

	// Test connection
	if err := c.testConnection(); err != nil {
		return nil, fmt.Errorf("failed to connect to Valkey: %w", err)
	}

	logger.Info("Valkey consumer created successfully",
		"address", config.Address,
		"database", config.Database,
		"max_concurrent_messages", options.MaxConcurrentMessages,
		"component", "valkeyreceiver")

	return c, nil
}

// Subscribe registers a handler for a specific queue
func (c *consumer) Subscribe(queue string, handler MessageHandler) error {
	if queue == "" {
		return fmt.Errorf("queue name cannot be empty")
	}
	if handler == nil {
		return fmt.Errorf("handler cannot be nil")
	}

	c.subMutex.Lock()
	defer c.subMutex.Unlock()

	c.subscriptions[queue] = handler
	c.logger.Info("Subscribed to queue",
		"queue", queue,
		"total_subscriptions", len(c.subscriptions),
		"component", "valkeyreceiver")

	// Call rebalance handler if provided
	if c.options.RebalanceHandler != nil {
		c.options.RebalanceHandler(queue)
	}

	return nil
}

// Start begins consuming messages (blocking operation)
func (c *consumer) Start(ctx context.Context) error {
	if c.isRunning.Load() {
		return fmt.Errorf("consumer is already running")
	}

	c.isRunning.Store(true)
	c.startTime = time.Now()
	c.connectionState.Store(int32(StateConnecting))

	c.logger.Info("Starting Valkey consumer",
		"queues", c.getQueueNames(),
		"max_concurrent_messages", c.options.MaxConcurrentMessages,
		"component", "valkeyreceiver")

	// Test connection before starting
	if err := c.testConnection(); err != nil {
		c.isRunning.Store(false)
		return fmt.Errorf("failed to establish connection: %w", err)
	}

	c.connectionState.Store(int32(StateConnected))

	defer func() {
		c.isRunning.Store(false)
		close(c.stoppedChan)
		c.logger.Info("Consumer stopped", "component", "valkeyreceiver")
	}()

	// Start message consumption loop
	for {
		select {
		case <-ctx.Done():
			c.logger.Info("Context cancelled, stopping consumer", "component", "valkeyreceiver")
			return ctx.Err()
		case <-c.stopChan:
			c.logger.Info("Stop signal received, stopping consumer", "component", "valkeyreceiver")
			return nil
		default:
			if err := c.consumeMessages(ctx); err != nil {
				c.logger.Error("Error in message consumption loop",
					"error", err,
					"component", "valkeyreceiver")
				
				// Increment error counter
				c.connectionErrors.Add(1)
				
				// Call error handler if provided
				if c.options.ErrorHandler != nil {
					c.options.ErrorHandler(err)
				}

				// Wait before retrying
				select {
				case <-time.After(c.config.RetryDelay):
				case <-ctx.Done():
					return ctx.Err()
				case <-c.stopChan:
					return nil
				}
			}
		}
	}
}

// consumeMessages handles the main consumption loop
func (c *consumer) consumeMessages(ctx context.Context) error {
	queues := c.getQueueNames()
	if len(queues) == 0 {
		c.logger.Warn("No queues subscribed, waiting...", "component", "valkeyreceiver")
		time.Sleep(5 * time.Second)
		return nil
	}

	// Convert queue names to Redis keys (add queue: prefix)
	redisKeys := make([]string, len(queues))
	for i, queue := range queues {
		redisKeys[i] = fmt.Sprintf("queue:%s", queue)
	}

	// Use circuit breaker to protect against Redis failures
	result, err := c.circuitBreaker.Execute(func() (interface{}, error) {
		// Rate limit the operation
		if err := c.rateLimiter.Wait(ctx); err != nil {
			return nil, err
		}

		// BRPOP with timeout for blocking consumption
		return c.client.BRPop(ctx, c.config.BlockingTimeout, redisKeys...).Result()
	})

	if err != nil {
		if err == redis.Nil {
			// No messages available, this is normal
			return nil
		}
		return fmt.Errorf("BRPOP failed: %w", err)
	}

	// Parse BRPOP result
	values, ok := result.([]string)
	if !ok || len(values) != 2 {
		return fmt.Errorf("unexpected BRPOP result format")
	}

	queueKey := values[0]   // e.g., "queue:user-registrations"
	messageData := values[1]

	// Extract queue name from Redis key
	queue := ""
	if len(queueKey) > 6 && queueKey[:6] == "queue:" {
		queue = queueKey[6:]
	}

	// Process the message
	return c.processMessage(ctx, queue, messageData)
}

// processMessage handles individual message processing
func (c *consumer) processMessage(ctx context.Context, queue string, messageData string) error {
	c.messagesReceived.Add(1)

	// Get handler for the queue
	c.subMutex.RLock()
	handler, exists := c.subscriptions[queue]
	c.subMutex.RUnlock()

	if !exists {
		c.logger.Warn("No handler for queue, dropping message",
			"queue", queue,
			"component", "valkeyreceiver")
		return nil
	}

	// Acquire semaphore for concurrent processing control
	select {
	case c.semaphore <- struct{}{}:
		defer func() { <-c.semaphore }()
	case <-ctx.Done():
		return ctx.Err()
	}

	// Process message in goroutine for concurrency
	c.processingWg.Add(1)
	go func() {
		defer c.processingWg.Done()
		c.handleMessage(ctx, queue, messageData, handler)
	}()

	return nil
}

// handleMessage processes a single message with timeout and error handling
func (c *consumer) handleMessage(ctx context.Context, queue string, messageData string, handler MessageHandler) {
	startTime := time.Now()
	messageID := uuid.New().String()

	// Create context with timeout for message processing
	processCtx, cancel := context.WithTimeout(ctx, c.options.ProcessingTimeout)
	defer cancel()

	c.logger.Debug("Processing message",
		"queue", queue,
		"message_id", messageID,
		"component", "valkeyreceiver")

	// Parse message envelope
	var envelope MessageEnvelope
	if err := json.Unmarshal([]byte(messageData), &envelope); err != nil {
		c.logger.Error("Failed to unmarshal message envelope",
			"error", err,
			"queue", queue,
			"message_id", messageID,
			"raw_data", messageData,
			"component", "valkeyreceiver")
		c.processingErrors.Add(1)
		return
	}

	// Create message object
	message := Message{
		Queue:     queue,
		Value:     []byte(messageData),
		Headers:   envelope.Headers,
		Timestamp: envelope.Timestamp,
		MessageID: envelope.ID,
		TTL:       envelope.TTL,
		Retries:   envelope.Retries,
	}

	// Process message with handler
	if err := handler.Handle(processCtx, message); err != nil {
		c.logger.Error("Message processing failed",
			"error", err,
			"queue", queue,
			"message_id", messageID,
			"retries", envelope.Retries,
			"component", "valkeyreceiver")

		c.processingErrors.Add(1)

		// Call error handler if provided
		if c.options.ErrorHandler != nil {
			c.options.ErrorHandler(err)
		}

		// Handle retries and dead letter queue
		c.handleFailedMessage(ctx, queue, messageData, envelope, err)
		return
	}

	// Success
	processingTime := time.Since(startTime)
	c.messagesProcessed.Add(1)

	c.logger.Debug("Message processed successfully",
		"queue", queue,
		"message_id", messageID,
		"processing_time", processingTime,
		"component", "valkeyreceiver")

	// Call success handler if provided
	if c.options.SuccessHandler != nil {
		c.options.SuccessHandler(message)
	}

	// Update metrics
	if c.options.MetricsHandler != nil {
		metrics := c.getMetrics()
		metrics.LastProcessingTime = processingTime
		c.options.MetricsHandler(metrics)
	}
}

// handleFailedMessage handles message retry logic and dead letter queue
func (c *consumer) handleFailedMessage(ctx context.Context, queue string, messageData string, envelope MessageEnvelope, err error) {
	// Check if we should retry
	if envelope.Retries < c.options.MaxRetries {
		// Increment retry count
		envelope.Retries++

		// Re-marshal the envelope
		retryData, marshalErr := json.Marshal(envelope)
		if marshalErr != nil {
			c.logger.Error("Failed to marshal retry message",
				"error", marshalErr,
				"queue", queue,
				"message_id", envelope.ID,
				"component", "valkeyreceiver")
			return
		}

		// Push back to queue for retry with delay
		go func() {
			time.Sleep(c.options.RetryBackoff)
			
			queueKey := fmt.Sprintf("queue:%s", queue)
			if pushErr := c.client.LPush(ctx, queueKey, string(retryData)).Err(); pushErr != nil {
				c.logger.Error("Failed to requeue message for retry",
					"error", pushErr,
					"queue", queue,
					"message_id", envelope.ID,
					"component", "valkeyreceiver")
			} else {
				c.logger.Info("Message requeued for retry",
					"queue", queue,
					"message_id", envelope.ID,
					"retry_attempt", envelope.Retries,
					"component", "valkeyreceiver")
			}
		}()
		return
	}

	// Send to dead letter queue if configured
	if c.options.DeadLetterQueue != "" {
		dlqKey := fmt.Sprintf("queue:%s", c.options.DeadLetterQueue)
		if pushErr := c.client.LPush(ctx, dlqKey, messageData).Err(); pushErr != nil {
			c.logger.Error("Failed to send message to dead letter queue",
				"error", pushErr,
				"queue", queue,
				"dead_letter_queue", c.options.DeadLetterQueue,
				"message_id", envelope.ID,
				"component", "valkeyreceiver")
		} else {
			c.logger.Info("Message sent to dead letter queue",
				"queue", queue,
				"dead_letter_queue", c.options.DeadLetterQueue,
				"message_id", envelope.ID,
				"final_error", err.Error(),
				"component", "valkeyreceiver")
		}
	} else {
		c.logger.Error("Message permanently failed processing (no dead letter queue configured)",
			"queue", queue,
			"message_id", envelope.ID,
			"final_error", err.Error(),
			"component", "valkeyreceiver")
	}
}

// Stop gracefully stops the consumer
func (c *consumer) Stop() error {
	if !c.isRunning.Load() {
		return fmt.Errorf("consumer is not running")
	}

	c.logger.Info("Stopping consumer gracefully", "component", "valkeyreceiver")

	// Signal stop
	close(c.stopChan)

	// Wait for processing to complete with timeout
	done := make(chan struct{})
	go func() {
		c.processingWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		c.logger.Info("All message processing completed", "component", "valkeyreceiver")
	case <-time.After(30 * time.Second):
		c.logger.Warn("Timeout waiting for message processing to complete", "component", "valkeyreceiver")
	}

	// Wait for main loop to stop
	select {
	case <-c.stoppedChan:
	case <-time.After(5 * time.Second):
		c.logger.Warn("Timeout waiting for consumer to stop", "component", "valkeyreceiver")
	}

	return nil
}

// Close releases all resources
func (c *consumer) Close() error {
	if c.isRunning.Load() {
		if err := c.Stop(); err != nil {
			c.logger.Warn("Error stopping consumer during close", "error", err, "component", "valkeyreceiver")
		}
	}

	if c.client != nil {
		if err := c.client.Close(); err != nil {
			c.logger.Error("Error closing Redis client", "error", err, "component", "valkeyreceiver")
			return err
		}
	}

	c.logger.Info("Consumer closed successfully", "component", "valkeyreceiver")
	return nil
}

// Health returns the current health status
func (c *consumer) Health() HealthStatus {
	status := "healthy"
	connectionState := ConnectionState(c.connectionState.Load()).String()
	
	// Determine overall health status
	if connectionState != "connected" {
		status = "unhealthy"
	} else if c.processingErrors.Load() > c.messagesProcessed.Load()/10 { // More than 10% error rate
		status = "degraded"
	}

	circuitBreakerState := "closed"
	switch c.circuitBreaker.State() {
	case gobreaker.StateHalfOpen:
		circuitBreakerState = "half-open"
	case gobreaker.StateOpen:
		circuitBreakerState = "open"
	}

	return HealthStatus{
		Status:            status,
		MessagesProcessed: c.messagesProcessed.Load(),
		ErrorCount:        c.processingErrors.Load(),
		LastMessageTime:   time.Now(), // TODO: Track actual last message time
		Uptime:            time.Since(c.startTime),
		ConnectionState:   connectionState,
		CircuitBreaker:    circuitBreakerState,
		QueueSizes:        c.getQueueSizes(),
	}
}

// GetQueueSize returns the number of messages in a queue
func (c *consumer) GetQueueSize(ctx context.Context, queue string) (int64, error) {
	queueKey := fmt.Sprintf("queue:%s", queue)
	return c.client.LLen(ctx, queueKey).Result()
}

// Helper methods

func (c *consumer) testConnection() error {
	ctx, cancel := context.WithTimeout(context.Background(), c.config.DialTimeout)
	defer cancel()

	c.connectionState.Store(int32(StateConnecting))
	
	if err := c.client.Ping(ctx).Err(); err != nil {
		c.connectionState.Store(int32(StateDisconnected))
		c.connectionErrors.Add(1)
		return err
	}

	c.connectionState.Store(int32(StateConnected))
	return nil
}

func (c *consumer) getQueueNames() []string {
	c.subMutex.RLock()
	defer c.subMutex.RUnlock()

	queues := make([]string, 0, len(c.subscriptions))
	for queue := range c.subscriptions {
		queues = append(queues, queue)
	}
	return queues
}

func (c *consumer) getQueueSizes() map[string]int64 {
	sizes := make(map[string]int64)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, queue := range c.getQueueNames() {
		if size, err := c.GetQueueSize(ctx, queue); err == nil {
			sizes[queue] = size
		}
	}
	return sizes
}

func (c *consumer) getMetrics() ConsumerMetrics {
	circuitBreakerState := "closed"
	switch c.circuitBreaker.State() {
	case gobreaker.StateHalfOpen:
		circuitBreakerState = "half-open"
	case gobreaker.StateOpen:
		circuitBreakerState = "open"
	}

	return ConsumerMetrics{
		MessagesReceived:    c.messagesReceived.Load(),
		MessagesProcessed:   c.messagesProcessed.Load(),
		ProcessingErrors:    c.processingErrors.Load(),
		CircuitBreakerState: circuitBreakerState,
		ConnectionErrors:    c.connectionErrors.Load(),
		ReconnectAttempts:   c.reconnectAttempts.Load(),
		// AvgProcessingTime and LastProcessingTime would need additional tracking
	}
}