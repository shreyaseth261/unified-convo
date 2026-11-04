package apperr

import (
	"errors"
	"fmt"
)

// ErrNotFound is returned when a conversation or message does not exist.
var ErrNotFound = errors.New("not found")

// ErrInvalid is returned when a request fails validation.
var ErrInvalid = errors.New("invalid")

// Invalid wraps a validation message so callers can use errors.Is.
func Invalid(msg string) error {
	return fmt.Errorf("%w: %s", ErrInvalid, msg)
}
