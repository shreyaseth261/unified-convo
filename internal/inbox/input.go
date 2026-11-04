package inbox

import (
	"strings"
	"time"

	"unifiedconvo/internal/apperr"
	"unifiedconvo/internal/model"
	"unifiedconvo/internal/status"
)

// InboundInput is a mocked provider webhook.
type InboundInput struct {
	ProviderMessageID string
	LocationID        string
	Channel           string
	From              string
	To                string
	Body              string
	SentAt            time.Time
}

func (in *InboundInput) validate() error {
	in.ProviderMessageID = strings.TrimSpace(in.ProviderMessageID)
	in.LocationID = strings.TrimSpace(in.LocationID)
	in.Channel = strings.ToLower(strings.TrimSpace(in.Channel))
	in.From = strings.TrimSpace(in.From)
	in.To = strings.TrimSpace(in.To)
	in.Body = strings.TrimSpace(in.Body)
	if in.ProviderMessageID == "" || len(in.ProviderMessageID) > 200 {
		return apperr.Invalid("providerMessageId is required")
	}
	if in.LocationID == "" || len(in.LocationID) > 200 {
		return apperr.Invalid("locationId is required")
	}
	if in.Channel != model.ChannelSMS && in.Channel != model.ChannelWhatsApp && in.Channel != model.ChannelEmail {
		return apperr.Invalid("channel must be sms, whatsapp, or email")
	}
	if in.From == "" || in.To == "" {
		return apperr.Invalid("from and to are required")
	}
	if in.Body == "" {
		return apperr.Invalid("body is required")
	}
	if len([]rune(in.Body)) > maxBodyRunes {
		return apperr.Invalid("body is too long")
	}
	if in.SentAt.IsZero() {
		return apperr.Invalid("sentAt is required")
	}
	return nil
}

// OutboundInput is an agent reply.
type OutboundInput struct {
	Channel          string
	Body             string
	LocationFallback string
}

func (in *OutboundInput) validate() error {
	in.Channel = strings.ToLower(strings.TrimSpace(in.Channel))
	in.Body = strings.TrimSpace(in.Body)
	if in.Channel != model.ChannelSMS && in.Channel != model.ChannelWhatsApp && in.Channel != model.ChannelEmail {
		return apperr.Invalid("channel must be sms, whatsapp, or email")
	}
	if in.Body == "" {
		return apperr.Invalid("body is required")
	}
	if len([]rune(in.Body)) > maxBodyRunes {
		return apperr.Invalid("body is too long")
	}
	return nil
}

// StatusInput is a provider delivery receipt.
type StatusInput struct {
	ProviderMessageID string
	Status            string
	Timestamp         time.Time
}

func (in *StatusInput) validate() error {
	in.ProviderMessageID = strings.TrimSpace(in.ProviderMessageID)
	in.Status = strings.ToLower(strings.TrimSpace(in.Status))
	if in.ProviderMessageID == "" {
		return apperr.Invalid("providerMessageId is required")
	}
	if !status.Known(in.Status) || in.Status == status.Queued {
		return apperr.Invalid("status must be sent, delivered, read, or failed")
	}
	if in.Timestamp.IsZero() {
		return apperr.Invalid("timestamp is required")
	}
	return nil
}

// ConversationFilter is the inbox list query.
type ConversationFilter struct {
	Status     string
	AssignedTo string
	Cursor     string
	Limit      int
}

// PatchInput updates assignment and conversation status.
// A non-nil AssignedTo of "" clears the assignee.
type PatchInput struct {
	AssignedTo *string
	Status     *string
}

func (p *PatchInput) validate() error {
	if p.Status == nil && p.AssignedTo == nil {
		return apperr.Invalid("status or assignedTo is required")
	}
	if p.Status != nil {
		v := strings.ToLower(strings.TrimSpace(*p.Status))
		if v != model.ConversationOpen && v != model.ConversationClosed {
			return apperr.Invalid("status must be open or closed")
		}
		p.Status = &v
	}
	if p.AssignedTo != nil {
		v := strings.TrimSpace(*p.AssignedTo)
		if len(v) > 200 {
			return apperr.Invalid("assignedTo is too long")
		}
		p.AssignedTo = &v
	}
	return nil
}
