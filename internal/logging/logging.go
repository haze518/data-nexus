package logging

import (
	"fmt"
	"io"
	"log"
	"sync"
)

// Level defines the severity level for log messages.
type Level int8

const (
	// DebugLevel logs fine-grained messages useful for debugging.
	DebugLevel Level = iota

	// InfoLevel logs general information about application progress.
	InfoLevel

	// WarnLevel logs potentially harmful situations.
	WarnLevel

	// ErrorLevel logs error events that might still allow the application to continue running.
	ErrorLevel
)

// Logger is a thread-safe structured logger with support for leveled logging.
type Logger struct {
	maxLevel Level        // Maximum log level to display
	out      *log.Logger  // Underlying Go logger
	mu       sync.RWMutex // Guards concurrent access to maxLevel
}

// NewLogger creates and returns a new Logger instance.
// The logger will only output messages at or above the specified level.
func NewLogger(level Level, output io.Writer) *Logger {
	return &Logger{
		maxLevel: level,
		out:      log.New(output, "", log.LstdFlags),
	}
}

// Debug logs a debug-level message if the logger is configured to show it.
func (l *Logger) Debug(args ...any) {
	if l.ok(DebugLevel) {
		l.out.Printf("[DEBUG] %v", fmt.Sprint(args...))
	}
}

// Info logs an info-level message if the logger is configured to show it.
func (l *Logger) Info(args ...any) {
	if l.ok(InfoLevel) {
		l.out.Printf("[INFO] %v", fmt.Sprint(args...))
	}
}

// Warn logs a warning-level message if the logger is configured to show it.
func (l *Logger) Warn(args ...any) {
	if l.ok(WarnLevel) {
		l.out.Printf("[WARN] %v", fmt.Sprint(args...))
	}
}

// Error logs an error-level message if the logger is configured to show it.
func (l *Logger) Error(args ...any) {
	if l.ok(ErrorLevel) {
		l.out.Printf("[ERROR] %v", fmt.Sprint(args...))
	}
}

// SetLevel updates the logger's minimum level. Only messages at or above
// this level will be logged.
func (l *Logger) SetLevel(level Level) {
	l.mu.Lock()
	l.maxLevel = level
	l.mu.Unlock()
}

// ok returns true if the given level is enabled for output.
func (l *Logger) ok(level Level) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return level >= l.maxLevel
}
