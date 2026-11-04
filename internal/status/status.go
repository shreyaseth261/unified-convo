// Package status owns the delivery lifecycle.
//
// Success states only move forward: queued → sent → delivered → read.
// A callback that arrives early (delivered before sent) is applied immediately,
// and the late earlier status is ignored.
//
// failed is terminal for the send pipeline. It can replace queued or sent.
// It does not overwrite delivered or read, and nothing replaces failed.
package status

const (
	Queued    = "queued"
	Sent      = "sent"
	Delivered = "delivered"
	Read      = "read"
	Failed    = "failed"
)

var rank = map[string]int{
	Queued:    1,
	Sent:      2,
	Delivered: 3,
	Read:      4,
}

// Known reports whether s is a provider status we accept.
func Known(s string) bool {
	switch s {
	case Queued, Sent, Delivered, Read, Failed:
		return true
	default:
		return false
	}
}

// Advance moves current to next when the lifecycle allows it.
// The boolean is false when next was ignored.
func Advance(current, next string) (string, bool) {
	if current == next || !Known(next) || !Known(current) {
		return current, false
	}
	if current == Failed {
		return current, false
	}
	if next == Failed {
		if current == Queued || current == Sent {
			return Failed, true
		}
		return current, false
	}
	if rank[next] > rank[current] {
		return next, true
	}
	return current, false
}
