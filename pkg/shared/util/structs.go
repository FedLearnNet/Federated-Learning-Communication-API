package util

import (
	"encoding/json"
	nethttp "net/http"
	"time"
)

type Health struct {
	Status string `json:"status"` // "UP" / "DOWN"
	Utc    string `json:"utc"`
}

func HandleHealthz(w nethttp.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(nethttp.StatusOK)
	_ = json.NewEncoder(w).Encode(Health{
		Status: "UP",
		Utc:    time.Now().UTC().Format(time.RFC3339),
	})
}
