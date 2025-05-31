package valkeyreceiver

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

// NewLogger creates a new structured logger with the specified level and output
func NewLogger(level slog.Level, logFile string) (*slog.Logger, error) {
	var writers []io.Writer
	writers = append(writers, os.Stdout)

	// Add file writer if log file is specified
	if logFile != "" {
		file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err != nil {
			return nil, err
		}
		writers = append(writers, file)
	}

	// Create multi-writer for both stdout and file
	multiWriter := io.MultiWriter(writers...)

	// Create JSON handler with custom options
	opts := &slog.HandlerOptions{
		Level: level,
		AddSource: level <= slog.LevelDebug,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			// Add component context to all log entries
			if a.Key == slog.TimeKey {
				return slog.Attr{Key: "timestamp", Value: a.Value}
			}
			if a.Key == slog.LevelKey {
				return slog.Attr{Key: "level", Value: a.Value}
			}
			if a.Key == slog.MessageKey {
				return slog.Attr{Key: "msg", Value: a.Value}
			}
			return a
		},
	}

	handler := slog.NewJSONHandler(multiWriter, opts)
	logger := slog.New(handler).With("component", "valkeyreceiver")

	return logger, nil
}

// ParseLogLevel converts string log level to slog.Level
func ParseLogLevel(level string) slog.Level {
	switch strings.ToUpper(level) {
	case "DEBUG":
		return slog.LevelDebug
	case "INFO":
		return slog.LevelInfo
	case "WARN", "WARNING":
		return slog.LevelWarn
	case "ERROR":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// LoggerWithContext adds contextual information to the logger
func LoggerWithContext(logger *slog.Logger, queue string, messageID string) *slog.Logger {
	return logger.With(
		"queue", queue,
		"message_id", messageID,
	)
}

// LogError logs an error with additional context
func LogError(logger *slog.Logger, msg string, err error, ctx ...any) {
	args := []any{"error", err}
	args = append(args, ctx...)
	logger.Error(msg, args...)
}

// LogInfo logs an info message with context
func LogInfo(logger *slog.Logger, msg string, ctx ...any) {
	logger.Info(msg, ctx...)
}

// LogDebug logs a debug message with context
func LogDebug(logger *slog.Logger, msg string, ctx ...any) {
	logger.Debug(msg, ctx...)
}

// LogWarn logs a warning message with context
func LogWarn(logger *slog.Logger, msg string, ctx ...any) {
	logger.Warn(msg, ctx...)
}