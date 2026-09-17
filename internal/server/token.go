package server

import (
	"fmt"
	"os"
	"strings"
)

// ReadToken reads a bearer token from a mounted Secret; only trailing/leading
// whitespace is stripped. Tokens are deliberately absent from command arguments.
func ReadToken(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read API bearer token: %w", err)
	}
	token := strings.TrimSpace(string(data))
	if len(token) < 32 {
		return "", fmt.Errorf("API bearer token must contain at least 32 characters")
	}
	if strings.ContainsFunc(token, func(r rune) bool { return r < 0x21 || r > 0x7e }) {
		return "", fmt.Errorf("API bearer token must contain only printable ASCII without whitespace")
	}
	return token, nil
}
