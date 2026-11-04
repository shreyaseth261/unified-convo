// Package provider is the outbound channel boundary.
// Twilio, email, and WhatsApp are represented by this interface and a mock.
package provider

import "context"

// Outbound is a single send attempt.
type Outbound struct {
	MessageID      string
	ConversationID string
	LocationID     string
	Channel        string
	From           string
	To             string
	Body           string
}

// Sender delivers an outbound message and returns the provider's id.
type Sender interface {
	Send(ctx context.Context, msg Outbound) (providerMessageID string, err error)
}
