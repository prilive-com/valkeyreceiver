package valkeyreceiver

import (
	"context"
	"encoding/json"
	"fmt"
)

// CreateJSONHandler creates a generic handler for JSON messages
func CreateJSONHandler[T any](handler func(ctx context.Context, data T) error) MessageHandler {
	return MessageHandlerFunc(func(ctx context.Context, message Message) error {
		// Parse the message envelope first
		var envelope MessageEnvelope
		if err := json.Unmarshal(message.Value, &envelope); err != nil {
			return fmt.Errorf("failed to unmarshal message envelope: %w", err)
		}

		// Extract the payload and convert to the specified type
		payloadBytes, err := json.Marshal(envelope.Payload)
		if err != nil {
			return fmt.Errorf("failed to marshal payload: %w", err)
		}

		var data T
		if err := json.Unmarshal(payloadBytes, &data); err != nil {
			return fmt.Errorf("failed to unmarshal data to %T: %w", data, err)
		}

		// Call the actual handler
		return handler(ctx, data)
	})
}

// CreateRawHandler creates a handler that receives the raw message data
func CreateRawHandler(handler func(ctx context.Context, data []byte) error) MessageHandler {
	return MessageHandlerFunc(func(ctx context.Context, message Message) error {
		return handler(ctx, message.Value)
	})
}

// CreateLoggingHandler wraps a handler with logging capabilities
func CreateLoggingHandler(handler MessageHandler, logger interface{}) MessageHandler {
	return MessageHandlerFunc(func(ctx context.Context, message Message) error {
		// Log start of processing
		if l, ok := logger.(interface {
			Info(msg string, args ...any)
		}); ok {
			l.Info("Starting message processing",
				"queue", message.Queue,
				"message_id", message.MessageID,
				"component", "valkeyreceiver")
		}

		// Process the message
		err := handler.Handle(ctx, message)

		// Log result
		if err != nil {
			if l, ok := logger.(interface {
				Error(msg string, args ...any)
			}); ok {
				l.Error("Message processing failed",
					"error", err,
					"queue", message.Queue,
					"message_id", message.MessageID,
					"component", "valkeyreceiver")
			}
		} else {
			if l, ok := logger.(interface {
				Info(msg string, args ...any)
			}); ok {
				l.Info("Message processing completed",
					"queue", message.Queue,
					"message_id", message.MessageID,
					"component", "valkeyreceiver")
			}
		}

		return err
	})
}

// CreateValidationHandler wraps a handler with message validation
func CreateValidationHandler(handler MessageHandler, validator func(Message) error) MessageHandler {
	return MessageHandlerFunc(func(ctx context.Context, message Message) error {
		// Validate the message first
		if err := validator(message); err != nil {
			return fmt.Errorf("message validation failed: %w", err)
		}

		// Process the message
		return handler.Handle(ctx, message)
	})
}

// UnmarshalJSON is a helper function to unmarshal message data
func UnmarshalJSON[T any](message Message, target *T) error {
	var envelope MessageEnvelope
	if err := json.Unmarshal(message.Value, &envelope); err != nil {
		return fmt.Errorf("failed to unmarshal message envelope: %w", err)
	}

	payloadBytes, err := json.Marshal(envelope.Payload)
	if err != nil {
		return fmt.Errorf("failed to marshal payload: %w", err)
	}

	if err := json.Unmarshal(payloadBytes, target); err != nil {
		return fmt.Errorf("failed to unmarshal to %T: %w", target, err)
	}

	return nil
}