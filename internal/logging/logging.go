package logging

import (
	"fmt"
	"io"
	"log"
	"sync"
)

type Level int8

const (
	DebugLevel Level = iota
	InfoLevel
	WarnLevel
	ErrorLevel
)

type Logger struct {
	maxLevel Level
	out      *log.Logger
	mu       sync.RWMutex
}

func NewLogger(level Level, output io.Writer) *Logger {
	return &Logger{
		maxLevel: level,
		out:      log.New(output, "", log.LstdFlags),
	}
}

func (l *Logger) Debug(args ...any) {
	if l.ok(DebugLevel) {
		l.out.Printf("[DEBUG] %v", fmt.Sprint(args...))
	}
}

func (l *Logger) Info(args ...any) {
	if l.ok(InfoLevel) {
		l.out.Printf("[INFO] %v", fmt.Sprint(args...))
	}
}

func (l *Logger) Warn(args ...any) {
	if l.ok(WarnLevel) {
		l.out.Printf("[WARN] %v", fmt.Sprint(args...))
	}
}

func (l *Logger) Error(args ...any) {
	if l.ok(ErrorLevel) {
		l.out.Printf("[ERROR] %v", fmt.Sprint(args...))
	}
}

func (l *Logger) SetLevel(level Level) {
	l.mu.Lock()
	l.maxLevel = level
	l.mu.Unlock()
}

func (l *Logger) ok(level Level) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return level >= l.maxLevel
}
