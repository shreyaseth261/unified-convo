package cursor

import (
	"encoding/base64"
	"fmt"
	"strings"
)

// Encode packs a sort timestamp and id into an opaque cursor.
func Encode(ts, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(ts + "|" + id))
}

// Decode reverses Encode. An empty cursor is valid and means "start".
func Decode(s string) (ts, id string, err error) {
	if s == "" {
		return "", "", nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return "", "", fmt.Errorf("cursor: %w", err)
	}
	ts, id, ok := strings.Cut(string(raw), "|")
	if !ok || ts == "" || id == "" {
		return "", "", fmt.Errorf("cursor: malformed")
	}
	return ts, id, nil
}
