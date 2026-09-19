package main

import (
	"io"
	"testing"
)

func TestDiscoveryAdmissionFlags(t *testing.T) {
	flags := []string{"--max-concurrent-streams", "--max-discovery-connections", "--max-discovery-rpcs", "--max-discovery-watches", "--initial-request-timeout"}
	for _, flag := range flags {
		t.Run(flag, func(t *testing.T) {
			good, bad := "64", "-1"
			if flag == "--initial-request-timeout" {
				good, bad = "500ms", "-1s"
			}
			s, err := parseSettings([]string{"--insecure", flag, good}, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			if err = s.validate(); err != nil {
				t.Fatal(err)
			}
			s, err = parseSettings([]string{"--insecure", flag, bad}, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			if s.validate() == nil {
				t.Fatal("invalid admission setting accepted")
			}
		})
	}
}
