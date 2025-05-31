package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prilive-com/valkeyreceiver/valkeyreceiver"
)

// ExampleUserData demonstrates a custom data structure
type ExampleUserData struct {
	Name             string    `json:"name"`
	Email            string    `json:"email"`
	TelegramUserID   int64     `json:"telegram_user_id"`
	RegistrationTime time.Time `json:"registration_time"`
	Source           string    `json:"source"`
}

func main() {
	fmt.Println("🚀 Starting Valkey Receiver Demo")

	// Load configuration from environment variables
	config, err := valkeyreceiver.LoadConfig()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// Create logger
	logger, err := valkeyreceiver.NewLogger(slog.LevelInfo, "logs/demo-receiver.log")
	if err != nil {
		log.Fatalf("Failed to create logger: %v", err)
	}

	// Create consumer options
	options := &valkeyreceiver.ConsumerOptions{
		Logger: logger,
		ErrorHandler: func(err error) {
			logger.Error("Consumer error occurred", "error", err, "component", "demo")
		},
		SuccessHandler: func(message valkeyreceiver.Message) {
			logger.Info("Message processed successfully",
				"queue", message.Queue,
				"message_id", message.MessageID,
				"component", "demo")
		},
		MetricsHandler: func(metrics valkeyreceiver.ConsumerMetrics) {
			logger.Debug("Consumer metrics",
				"messages_received", metrics.MessagesReceived,
				"messages_processed", metrics.MessagesProcessed,
				"processing_errors", metrics.ProcessingErrors,
				"component", "demo")
		},
		MaxConcurrentMessages: 5,
		ProcessingTimeout:      30 * time.Second,
		DeadLetterQueue:        "failed-messages",
		MaxRetries:             3,
	}

	// Create consumer
	consumer, err := valkeyreceiver.NewConsumer(config, options)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	// Create different types of message handlers

	// 1. Generic JSON handler for user registration data
	userRegistrationHandler := valkeyreceiver.CreateJSONHandler(func(ctx context.Context, userData ExampleUserData) error {
		logger.Info("Processing user registration",
			"name", userData.Name,
			"email", userData.Email,
			"telegram_user_id", userData.TelegramUserID,
			"source", userData.Source,
			"component", "demo")

		// Simulate processing work
		time.Sleep(100 * time.Millisecond)

		// Here you would typically:
		// - Validate the data
		// - Store in database
		// - Call external APIs (like FreeIPA)
		// - Send notifications
		
		fmt.Printf("✅ Processed registration for %s (%s)\n", userData.Name, userData.Email)
		return nil
	})

	// 2. Raw message handler
	rawHandler := valkeyreceiver.CreateRawHandler(func(ctx context.Context, data []byte) error {
		logger.Info("Processing raw message", "size", len(data), "component", "demo")
		fmt.Printf("📦 Received raw message: %s\n", string(data))
		return nil
	})

	// 3. Generic handler with logging wrapper
	genericHandler := valkeyreceiver.CreateLoggingHandler(
		valkeyreceiver.MessageHandlerFunc(func(ctx context.Context, message valkeyreceiver.Message) error {
			logger.Info("Processing generic message",
				"queue", message.Queue,
				"message_id", message.MessageID,
				"component", "demo")

			// Parse any JSON data
			var data map[string]interface{}
			if err := json.Unmarshal(message.Value, &data); err == nil {
				fmt.Printf("🔍 Generic message data: %+v\n", data)
			}

			return nil
		}),
		logger,
	)

	// Subscribe to different queues with different handlers
	if err := consumer.Subscribe("user-registrations", userRegistrationHandler); err != nil {
		log.Fatalf("Failed to subscribe to user-registrations: %v", err)
	}

	if err := consumer.Subscribe("raw-messages", rawHandler); err != nil {
		log.Fatalf("Failed to subscribe to raw-messages: %v", err)
	}

	if err := consumer.Subscribe("generic-queue", genericHandler); err != nil {
		log.Fatalf("Failed to subscribe to generic-queue: %v", err)
	}

	// Set up graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start health monitoring in background
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				health := consumer.Health()
				logger.Info("Consumer health check",
					"status", health.Status,
					"messages_processed", health.MessagesProcessed,
					"error_count", health.ErrorCount,
					"uptime", health.Uptime,
					"connection_state", health.ConnectionState,
					"component", "demo")

				// Print queue sizes
				for queue, size := range health.QueueSizes {
					fmt.Printf("📊 Queue %s: %d messages\n", queue, size)
				}

			case <-ctx.Done():
				return
			}
		}
	}()

	// Start consumer in background
	go func() {
		logger.Info("Starting consumer", "component", "demo")
		if err := consumer.Start(ctx); err != nil && err != context.Canceled {
			logger.Error("Consumer exited with error", "error", err, "component", "demo")
		}
	}()

	fmt.Println("✅ Consumer started successfully!")
	fmt.Println("📋 Subscribed to queues:")
	fmt.Println("   - user-registrations (JSON handler)")
	fmt.Println("   - raw-messages (raw handler)")
	fmt.Println("   - generic-queue (generic handler)")
	fmt.Println()
	fmt.Println("💡 To test, send messages to these queues using redis-cli:")
	fmt.Println("   redis-cli LPUSH queue:user-registrations '{\"id\":\"123\",\"timestamp\":\"2025-05-30T10:00:00Z\",\"payload\":{\"name\":\"John Doe\",\"email\":\"john@example.com\",\"telegram_user_id\":123456789,\"registration_time\":\"2025-05-30T10:00:00Z\",\"source\":\"demo\"}}'")
	fmt.Println("   redis-cli LPUSH queue:raw-messages 'Hello World!'")
	fmt.Println("   redis-cli LPUSH queue:generic-queue '{\"message\":\"test\",\"data\":\"example\"}'")
	fmt.Println()
	fmt.Println("🛑 Press Ctrl+C to stop gracefully")

	// Wait for shutdown signal
	<-sigChan
	fmt.Println("\n🛑 Shutdown signal received, stopping gracefully...")

	// Cancel context to stop consumer
	cancel()

	// Give consumer time to stop gracefully
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	done := make(chan struct{})
	go func() {
		consumer.Stop()
		close(done)
	}()

	select {
	case <-done:
		fmt.Println("✅ Consumer stopped gracefully")
	case <-shutdownCtx.Done():
		fmt.Println("⚠️ Consumer shutdown timeout")
	}

	fmt.Println("👋 Demo completed!")
}