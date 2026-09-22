// Package server exposes a tiny HTTP endpoint for health, metrics, and job
// progress. It is disabled unless SEAM_HTTP_ADDR is set.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"example.com/seam/internal/checkpoint"
	"example.com/seam/internal/telemetry"
)

type Server struct {
	addr    string
	metrics *telemetry.Metrics
	cpStore *checkpoint.Store
	jobID   string
}

// Start builds the health/metrics/progress HTTP server and runs it until ctx is
// cancelled.ListenAndServe failures are logged, never fatal.
func Start(ctx context.Context, addr string, metrics *telemetry.Metrics, cpStore *checkpoint.Store, jobID string) {
	mux := http.NewServeMux()
	s := &apiHandler{metrics: metrics, cpStore: cpStore, jobID: jobID}
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/metrics", s.metricsOut)
	mux.HandleFunc("/progress", s.progress)
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		_ = srv.Close()
	}()
	go func() {
		log.Printf("seam: http server listening on %s (/healthz /metrics /progress)", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("seam: http server error: %v", err)
		}
	}()
}

type apiHandler struct {
	metrics *telemetry.Metrics
	cpStore *checkpoint.Store
	jobID   string
}

func (a *apiHandler) metricsOut(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	_, _ = fmt.Fprintf(w, "seam_chunks_completed %d\n", a.metrics.ChunksCompleted())
	_, _ = fmt.Fprintf(w, "seam_candidates_seen %d\n", a.metrics.CandidatesSeen())
	_, _ = fmt.Fprintf(w, "seam_survivors_written %d\n", a.metrics.SurvivorsWritten())
	_, _ = fmt.Fprintf(w, "seam_cdc_events_applied %d\n", a.metrics.CDCEventsApplied())
}

func (a *apiHandler) progress(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	cp, err := a.cpStore.LoadCheckpoint(r.Context(), a.jobID)
	if err != nil || cp == nil {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "checkpoint not available"})
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"job_id":             cp.JobID,
		"generation":         cp.Generation,
		"attempt":            cp.Attempt,
		"scan_upper_bound":   cp.ScanUpperBound,
		"completed_through":  cp.CompletedThrough,
		"next_kafka_offset":  cp.NextKafkaOffset,
		"last_applied_lsn":   cp.LastAppliedLSN,
		"chunks_completed":   a.metrics.ChunksCompleted(),
		"candidates_seen":    a.metrics.CandidatesSeen(),
		"survivors_written":  a.metrics.SurvivorsWritten(),
		"cdc_events_applied": a.metrics.CDCEventsApplied(),
	})
}
