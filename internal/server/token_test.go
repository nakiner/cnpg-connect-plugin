package server

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestReadToken(t *testing.T) {
	path := filepath.Join(t.TempDir(), "token")
	if _, err := ReadToken(path); err == nil {
		t.Fatal("missing token accepted")
	}
	for _, token := range []string{"", "short", strings.Repeat("a", 32) + " " + strings.Repeat("b", 32), strings.Repeat("a", 32) + "\x00", strings.Repeat("a", 32) + "é"} {
		writeFile(t, path, []byte(token))
		if _, err := ReadToken(path); err == nil {
			t.Fatal("invalid token accepted")
		}
	}
	want := strings.Repeat("a", 64)
	writeFile(t, path, []byte("\n"+want+"\n"))
	got, err := ReadToken(path)
	if err != nil || got != want {
		t.Fatalf("valid token rejected or not trimmed: %v", err)
	}
}
