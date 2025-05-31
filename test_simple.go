package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/prilive-com/valkeyreceiver/valkeyreceiver"
)

func main() {
	fmt.Println("🧪 Valkey Receiver Simple Test")

	// Create simple config for testing
	config := &valkeyreceiver.Config{
		Address:                "10.1.0.4:30379",
		Password:               "7Xwdz01BYEu6p74sNRHf8He2",
		Database:               0,
		DialTimeout:            5 * time.Second,
		ReadTimeout:            3 * time.Second,
		WriteTimeout:           3 * time.Second,
		DefaultQueues:          []string{"test-queue"},
		ConsumerGroup:          "test-group",
		BlockingTimeout:        1 * time.Second,
		MaxConcurrentMessages:  5,
		ProcessingTimeout:      10 * time.Second,
		PoolSize:               10,
		MinIdleConns:           2,
		MaxIdleTime:            5 * time.Minute,
		ConnMaxLifetime:        1 * time.Hour,
		MaxRetries:             3,
		RetryDelay:             1 * time.Second,
		BreakerMaxRequests:     5,
		BreakerInterval:        2 * time.Minute,
		BreakerTimeout:         60 * time.Second,
		RateLimitRequests:      1000,
		RateLimitBurst:         2000,
		LogLevel:               "INFO",
	}

	// Create consumer
	consumer, err := valkeyreceiver.NewConsumer(config, nil)
	if err != nil {
		log.Fatalf("Failed to create consumer: %v", err)
	}
	defer consumer.Close()

	fmt.Println("✅ Consumer created successfully!")

	// Test connection by checking health
	health := consumer.Health()
	fmt.Printf("🏥 Health Status: %s\n", health.Status)
	fmt.Printf("🔗 Connection: %s\n", health.ConnectionState)

	// Subscribe to a test queue
	handler := valkeyreceiver.MessageHandlerFunc(func(ctx context.Context, message valkeyreceiver.Message) error {
		fmt.Printf("📨 Received message from queue '%s': %s\n", message.Queue, string(message.Value))
		return nil
	})

	if err := consumer.Subscribe("test-queue", handler); err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}

	fmt.Println("✅ Subscribed to test-queue")

	// Check queue size
	ctx := context.Background()
	size, err := consumer.GetQueueSize(ctx, "test-queue")
	if err != nil {
		fmt.Printf("⚠️ Could not get queue size: %v\n", err)
	} else {
		fmt.Printf("📊 Queue 'test-queue' size: %d messages\n", size)
	}

	// Test for a short time
	fmt.Println("🔄 Testing message consumption for 10 seconds...")
	fmt.Println("💡 To test, run: redis-cli -h 10.1.0.4 -p 30379 -a 7Xwdz01BYEu6p74sNRHf8He2 LPUSH queue:test-queue 'Hello from test!'")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start consumer in background
	go func() {
		if err := consumer.Start(ctx); err != nil && err != context.DeadlineExceeded {
			fmt.Printf("❌ Consumer error: %v\n", err)
		}
	}()

	// Wait for timeout
	<-ctx.Done()

	fmt.Println("✅ Test completed!")
	
	// Final health check
	finalHealth := consumer.Health()
	fmt.Printf("📊 Final Stats:\n")
	fmt.Printf("   - Messages Processed: %d\n", finalHealth.MessagesProcessed)
	fmt.Printf("   - Error Count: %d\n", finalHealth.ErrorCount)
	fmt.Printf("   - Uptime: %v\n", finalHealth.Uptime)
	fmt.Printf("   - Connection: %s\n", finalHealth.ConnectionState)
}