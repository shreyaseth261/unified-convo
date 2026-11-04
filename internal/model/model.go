package model

// Contact is a person at one location, keyed by a normalized phone or email.
type Contact struct {
	ID       string `json:"id"`
	Kind     string `json:"kind"`
	Value    string `json:"value"`
	Display  string `json:"display"`
	Identity string `json:"-"`
}

// Conversation is the single thread for a contact at a location, across channels.
type Conversation struct {
	ID                 string  `json:"id"`
	LocationID         string  `json:"locationId"`
	Status             string  `json:"status"`
	AssignedTo         *string `json:"assignedTo"`
	LastMessageAt      string  `json:"lastMessageAt"`
	LastMessagePreview string  `json:"lastMessagePreview"`
	UnreadCount        int     `json:"unreadCount"`
	CreatedAt          string  `json:"createdAt"`
	UpdatedAt          string  `json:"updatedAt"`
	Contact            Contact `json:"contact"`
}

// Message is one inbound or outbound payload.
type Message struct {
	ID                string `json:"id"`
	ConversationID    string `json:"conversationId"`
	LocationID        string `json:"locationId"`
	Direction         string `json:"direction"`
	Channel           string `json:"channel"`
	Body              string `json:"body"`
	From              string `json:"from"`
	To                string `json:"to"`
	ProviderMessageID string `json:"providerMessageId,omitempty"`
	Status            string `json:"status"`
	AttemptCount      int    `json:"attemptCount"`
	SentAt            string `json:"sentAt"`
	CreatedAt         string `json:"createdAt"`
	StatusUpdatedAt   string `json:"statusUpdatedAt,omitempty"`
}

const (
	DirectionInbound   = "inbound"
	DirectionOutbound  = "outbound"
	ConversationOpen   = "open"
	ConversationClosed = "closed"
	ChannelSMS         = "sms"
	ChannelWhatsApp    = "whatsapp"
	ChannelEmail       = "email"
)
