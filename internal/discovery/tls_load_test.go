//go:build loadtest

package discovery

import (
	"os"
	"strconv"
	"testing"
)

// CNPG_STREAM_CLIENTS selects 1..500 independent TLS connections. Keep this
// opt-in: client and server together require at least two file descriptors per
// connection and compete for CPU, so this is not a network capacity guarantee.
func TestTLSClientLoad(t *testing.T) {
	clients := 256
	if value := os.Getenv("CNPG_STREAM_CLIENTS"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > 500 {
			t.Fatal("CNPG_STREAM_CLIENTS must be between 1 and 500")
		}
		clients = parsed
	}
	runTLSFanout(t, clients, 30)
}
