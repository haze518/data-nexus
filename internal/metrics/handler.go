package metrics

import (
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/haze518/data-nexus/internal/storage"
)

func Handler(storage storage.Storage, writeCh chan<- []string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if storage.Len() == 0 {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		idsToAck := make([]string, 0, storage.Len())
		for {
			m, ok := storage.Pop()
			if !ok {
				break
			}
			if m.ID != nil {
				idsToAck = append(idsToAck, *m.ID)
			}

			fmt.Fprintf(w, "# HELP %s Automatically exported metric\n", m.Name)
			fmt.Fprintf(w, "# TYPE %s %s\n", m.Name, m.Type)

			label := ""
			if len(m.Labels) > 0 {
				labelPairs := make([]string, 0, len(m.Labels))
				for k, v := range m.Labels {
					labelPairs = append(labelPairs, fmt.Sprintf(`%s="%s"`, k, v))
				}
				label = "{" + strings.Join(labelPairs, ",") + "}"
			}
			ts := m.Timestamp.UnixNano() / int64(time.Millisecond)
			fmt.Fprintf(w, "%s%s %v %d\n", m.Name, label, m.Value, ts)
		}
		go func() {
			writeCh <-idsToAck
		}()
    })
}
