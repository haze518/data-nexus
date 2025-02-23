package logging

import (
	"bytes"
	"testing"
)

func TestLoggerFiltering(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(InfoLevel, &buf)

	logger.Debug("Debug message") // Should not be logged
	logger.Info("Info message")   // Should be logged
	logger.Warn("Warn message")   // Should be logged
	logger.Error("Error message") // Should be logged

	output := buf.String()

	if contains(output, "[DEBUG]") {
		t.Errorf("Expected DEBUG log not to be recorded, but it is present in the output")
	}
	if !contains(output, "[INFO] Info message") {
		t.Errorf("Expected INFO log to be recorded, but it is missing from the output")
	}
	if !contains(output, "[WARN] Warn message") {
		t.Errorf("Expected WARN log to be recorded, but it is missing from the output")
	}
	if !contains(output, "[ERROR] Error message") {
		t.Errorf("Expected ERROR log to be recorded, but it is missing from the output")
	}
}

func TestLoggerSetLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(WarnLevel, &buf)

	logger.Info("Info message")   // Should not be logged initially
	logger.Warn("Warn message")   // Should be logged
	logger.Error("Error message") // Should be logged

	outputBefore := buf.String()

	if contains(outputBefore, "[INFO]") {
		t.Errorf("INFO log should not be recorded before changing the level")
	}

	// Change log level to Debug
	logger.SetLevel(DebugLevel)
	logger.Debug("Debug message")
	logger.Info("Info message")

	outputAfter := buf.String()

	if !contains(outputAfter, "[DEBUG] Debug message") {
		t.Errorf("Expected DEBUG log after SetLevel(DebugLevel) but it is missing in the output")
	}
	if !contains(outputAfter, "[INFO] Info message") {
		t.Errorf("Expected INFO log after SetLevel(DebugLevel) but it is missing in the output")
	}
}

func TestLoggerFormat(t *testing.T) {
	var buf bytes.Buffer
	logger := NewLogger(DebugLevel, &buf)

	logger.Debug("Test message")
	output := buf.String()

	expected := "[DEBUG] Test message"
	if !contains(output, expected) {
		t.Errorf("Log format is incorrect: expected '%s', but got '%s'", expected, output)
	}
}

func contains(output, expected string) bool {
	return bytes.Contains([]byte(output), []byte(expected))
}
