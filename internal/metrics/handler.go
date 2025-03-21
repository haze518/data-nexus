package metrics

import (
	"bufio"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/haze518/data-nexus/internal/storage"
)

// Handler returns an HTTP handler that exposes collected metrics in Prometheus format.
// It drains the current metrics from the provided storage and writes them to the response
// in the Prometheus exposition format (text/plain; version=0.0.4).
//
// After successful write, it asynchronously sends the IDs of acknowledged metrics
// to the writeCh channel for further processing (e.g., acking in the broker).
//
// The handler responds with HTTP 200 OK even if there are no metrics to export.
func Handler(storage storage.Storage, writeCh chan<- []string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		metrics, ok := storage.Drain()
		if !ok || len(metrics) == 0 {
			w.WriteHeader(http.StatusOK)
			return
		}

		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		idsToAck := make([]string, 0, len(metrics))

		bw := bufio.NewWriter(w)
		defer bw.Flush()

		var sb strings.Builder

		for _, m := range metrics {
			// Collect IDs for acknowledgment
			if m.ID != nil {
				idsToAck = append(idsToAck, *m.ID)
			}

			// Write HELP and TYPE metadata
			fmt.Fprintf(bw, "# HELP %s Automatically exported metric\n", m.Name)
			fmt.Fprintf(bw, "# TYPE %s %s\n", m.Name, m.Type)

			// Build label string if any labels exist
			sb.Reset()
			sb.WriteByte('{')
			i := 0
			for k, v := range m.Labels {
				if i > 0 {
					sb.WriteByte(',')
				}
				fmt.Fprintf(&sb, `%s="%s"`, k, v)
				i++
			}
			sb.WriteByte('}')
			label := ""
			if i > 0 {
				label = sb.String()
			}

			// Convert timestamp to milliseconds
			ts := m.Timestamp.UnixNano() / int64(time.Millisecond)

			// Write metric line
			fmt.Fprintf(bw, "%s%s %v %d\n", m.Name, label, m.Value, ts)
		}

		// Acknowledge collected IDs asynchronously
		go func() {
			writeCh <- idsToAck
		}()
	})
}
