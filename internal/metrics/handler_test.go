package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/haze518/data-nexus/internal/storage"
	"github.com/haze518/data-nexus/internal/types"
)

func TestHandler_EmptyStorage(t *testing.T) {
	memStore := storage.NewInMemoryStorage()
	writeCh := make(chan []string, 1)

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()

	handler := Handler(memStore, writeCh)
	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "" {
		t.Errorf("Expected empty body, got %q", string(body))
	}
}

func TestHandler_WithMetrics(t *testing.T) {
	memStore := storage.NewInMemoryStorage()
	writeCh := make(chan []string, 1)

	now := time.Now()

	id := "123"
	memStore.Insert(&types.Metric{
		Name:      "cpu_usage",
		Value:     42.5,
		Type:      "gauge",
		Timestamp: now,
		Labels:    map[string]string{"instance": "a", "env": "test"},
		ID:        &id,
	})

	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	w := httptest.NewRecorder()

	handler := Handler(memStore, writeCh)
	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("Expected status 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	output := string(body)

	if !strings.Contains(output, "# TYPE cpu_usage gauge") {
		t.Error("Missing TYPE line")
	}
	if !strings.Contains(output, `cpu_usage{instance="a",env="test"}`) {
		t.Error("Missing metric line with labels")
	}

	ids := <-writeCh
	if len(ids) != 1 || ids[0] != "123" {
		t.Errorf("Expected ack ID '123', got %v", ids)
	}
}
