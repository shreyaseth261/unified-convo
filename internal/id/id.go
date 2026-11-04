package id

import (
	"crypto/rand"
	"encoding/hex"
)

// New returns a random 32-character hex identifier.
func New() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
