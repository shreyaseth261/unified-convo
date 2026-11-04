package inbox

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"

	"unifiedconvo/internal/apperr"
	"unifiedconvo/internal/cursor"
	"unifiedconvo/internal/id"
	"unifiedconvo/internal/model"
	"unifiedconvo/internal/normalize"
	"unifiedconvo/internal/provider"
	"unifiedconvo/internal/realtime"
	"unifiedconvo/internal/status"
	"unifiedconvo/internal/store"
	"unifiedconvo/internal/timefmt"
)

const (
	defaultLimit = 20
	maxLimit     = 100
	maxBodyRunes = 10000
	previewRunes = 140
)

// Config controls send retries. SyncSend runs the provider before the HTTP
// handler returns, which tests use to avoid sleeping on the async queue.
type Config struct {
	RetryBase time.Duration
	SyncSend  bool
	Workers   int
}

// Service is the inbox use-case layer.
type Service struct {
	store  *store.Store
	sender provider.Sender
	hub    *realtime.Hub
	cfg    Config
	jobs   chan string
}

// InboundResult is the outcome of a webhook, including retries of the same provider id.
type InboundResult struct {
	Duplicate    bool               `json:"duplicate"`
	Message      model.Message      `json:"message"`
	Conversation model.Conversation `json:"conversation"`
}

// StatusResult is the outcome of a delivery callback.
type StatusResult struct {
	Pending bool           `json:"pending"`
	Changed bool           `json:"changed"`
	Message *model.Message `json:"message,omitempty"`
}

// SendResult is the accepted outbound message and the conversation it belongs to.
type SendResult struct {
	Message      model.Message      `json:"message"`
	Conversation model.Conversation `json:"conversation"`
}

// Page is a cursor page of conversations or messages.
type Page[T any] struct {
	Data       []T    `json:"data"`
	NextCursor string `json:"-"`
}

// New wires the service. When SyncSend is false, a small worker pool drains sends.
func New(st *store.Store, sender provider.Sender, hub *realtime.Hub, cfg Config) *Service {
	if cfg.RetryBase <= 0 {
		cfg.RetryBase = 200 * time.Millisecond
	}
	if cfg.Workers <= 0 {
		cfg.Workers = 4
	}
	if sender == nil {
		sender = provider.NewMock(0.2)
	}
	s := &Service{store: st, sender: sender, hub: hub, cfg: cfg}
	if !cfg.SyncSend {
		s.jobs = make(chan string, 256)
		for i := 0; i < cfg.Workers; i++ {
			go s.worker()
		}
	}
	return s
}

func (s *Service) worker() {
	for messageID := range s.jobs {
		s.deliver(context.Background(), messageID)
	}
}

// Recover resumes outbound messages left queued by a previous process.
func (s *Service) Recover(ctx context.Context) error {
	ids, err := s.store.QueuedOutboundIDs(ctx)
	if err != nil {
		return err
	}
	for _, id := range ids {
		s.enqueue(ctx, id)
	}
	return nil
}

// Inbound stores a provider webhook once, finds or creates the contact and
// conversation, and reopens a closed thread.
func (s *Service) Inbound(ctx context.Context, in InboundInput) (InboundResult, error) {
	if err := in.validate(); err != nil {
		return InboundResult{}, err
	}
	identity, err := identityFor(in.Channel, in.From)
	if err != nil {
		return InboundResult{}, err
	}
	sentAt := timefmt.Format(in.SentAt)
	now := timefmt.Format(time.Now())
	body := strings.TrimSpace(in.Body)

	var result InboundResult
	duplicateInsert := false
	err = s.store.WithTx(ctx, func(tx *store.Tx) error {
		existing, err := tx.MessageByProviderID(ctx, in.ProviderMessageID)
		if err == nil {
			convo, cerr := tx.GetConversation(ctx, existing.ConversationID)
			if cerr != nil {
				return cerr
			}
			result = InboundResult{Duplicate: true, Message: existing, Conversation: convo}
			return nil
		}
		if err != apperr.ErrNotFound {
			return err
		}

		contact, err := tx.FindOrCreateContact(ctx, in.LocationID, identity, strings.TrimSpace(in.From), id.New(), now)
		if err != nil {
			return err
		}
		convoID, err := tx.FindOrCreateConversation(ctx, in.LocationID, contact.ID, id.New(), sentAt, now)
		if err != nil {
			return err
		}
		msg := model.Message{
			ID:                id.New(),
			ConversationID:    convoID,
			LocationID:        in.LocationID,
			Direction:         model.DirectionInbound,
			Channel:           in.Channel,
			Body:              body,
			From:              strings.TrimSpace(in.From),
			To:                strings.TrimSpace(in.To),
			ProviderMessageID: in.ProviderMessageID,
			Status:            status.Delivered,
			SentAt:            sentAt,
			CreatedAt:         now,
			StatusUpdatedAt:   now,
		}
		if err := tx.InsertMessage(ctx, msg); err != nil {
			if store.IsUnique(err) {
				duplicateInsert = true
				return errDuplicate
			}
			return err
		}
		if err := tx.TouchConversation(ctx, convoID, sentAt, preview(body), 1, true); err != nil {
			return err
		}
		convo, err := tx.GetConversation(ctx, convoID)
		if err != nil {
			return err
		}
		result = InboundResult{Message: msg, Conversation: convo}
		return nil
	})
	if err == errDuplicate || duplicateInsert {
		existing, gerr := s.store.MessageByProviderID(ctx, in.ProviderMessageID)
		if gerr != nil {
			return InboundResult{}, gerr
		}
		convo, gerr := s.store.GetConversation(ctx, existing.ConversationID)
		if gerr != nil {
			return InboundResult{}, gerr
		}
		return InboundResult{Duplicate: true, Message: existing, Conversation: convo}, nil
	}
	if err != nil {
		return InboundResult{}, err
	}
	s.publish(realtime.Event{Type: "message.created", LocationID: result.Message.LocationID, Data: result.Message})
	s.publish(realtime.Event{Type: "conversation.updated", LocationID: result.Conversation.LocationID, Data: result.Conversation})
	return result, nil
}

// Send queues an outbound message and delivers it through the mock provider.
func (s *Service) Send(ctx context.Context, conversationID string, in OutboundInput) (SendResult, error) {
	if err := in.validate(); err != nil {
		return SendResult{}, err
	}
	convo, err := s.store.GetConversation(ctx, conversationID)
	if err != nil {
		return SendResult{}, err
	}
	if !channelCompatible(convo.Contact.Kind, in.Channel) {
		return SendResult{}, apperr.Invalid("channel does not match the contact")
	}
	business := in.LocationFallback
	if business == "" {
		business = businessAddress(ctx, s.store, conversationID)
	}
	now := timefmt.Format(time.Now())
	body := strings.TrimSpace(in.Body)
	msg := model.Message{
		ID:              id.New(),
		ConversationID:  convo.ID,
		LocationID:      convo.LocationID,
		Direction:       model.DirectionOutbound,
		Channel:         in.Channel,
		Body:            body,
		From:            business,
		To:              convo.Contact.Value,
		Status:          status.Queued,
		SentAt:          now,
		CreatedAt:       now,
		StatusUpdatedAt: now,
	}
	err = s.store.WithTx(ctx, func(tx *store.Tx) error {
		if err := tx.InsertMessage(ctx, msg); err != nil {
			return err
		}
		return tx.TouchConversation(ctx, convo.ID, now, preview(body), 0, false)
	})
	if err != nil {
		return SendResult{}, err
	}
	convo, err = s.store.GetConversation(ctx, convo.ID)
	if err != nil {
		return SendResult{}, err
	}
	s.publish(realtime.Event{Type: "message.created", LocationID: msg.LocationID, Data: msg})
	s.publish(realtime.Event{Type: "conversation.updated", LocationID: convo.LocationID, Data: convo})

	s.enqueue(ctx, msg.ID)
	if s.cfg.SyncSend {
		msg, err = s.store.GetMessage(ctx, msg.ID)
		if err != nil {
			return SendResult{}, err
		}
		convo, err = s.store.GetConversation(ctx, convo.ID)
		if err != nil {
			return SendResult{}, err
		}
	}
	return SendResult{Message: msg, Conversation: convo}, nil
}

func (s *Service) enqueue(ctx context.Context, messageID string) {
	if s.cfg.SyncSend {
		s.deliver(ctx, messageID)
		return
	}
	s.jobs <- messageID
}

// Status applies a delivery callback. Unknown messages are remembered and
// merged when the outbound row is saved.
func (s *Service) Status(ctx context.Context, in StatusInput) (StatusResult, error) {
	if err := in.validate(); err != nil {
		return StatusResult{}, err
	}
	now := timefmt.Format(time.Now())
	eventTime := timefmt.Format(in.Timestamp)
	var result StatusResult
	err := s.store.WithTx(ctx, func(tx *store.Tx) error {
		msg, err := tx.MessageByProviderID(ctx, in.ProviderMessageID)
		if err == apperr.ErrNotFound {
			if err := tx.UpsertPending(ctx, in.ProviderMessageID, in.Status, eventTime, mergePending); err != nil {
				return err
			}
			msg, err = tx.MessageByProviderID(ctx, in.ProviderMessageID)
			if err == apperr.ErrNotFound {
				result.Pending = true
				return nil
			}
			if err != nil {
				return err
			}
		} else if err != nil {
			return err
		}
		next, changed := status.Advance(msg.Status, in.Status)
		if pending, err := tx.PendingStatus(ctx, in.ProviderMessageID); err != nil {
			return err
		} else if pending != "" {
			var also bool
			next, also = status.Advance(next, pending)
			changed = changed || also
		}
		if changed {
			if err := tx.ApplyDirectStatus(ctx, msg.ID, next, now); err != nil {
				return err
			}
		}
		if err := tx.DeletePending(ctx, in.ProviderMessageID); err != nil {
			return err
		}
		updated, err := tx.GetMessage(ctx, msg.ID)
		if err != nil {
			return err
		}
		result.Changed = changed
		result.Pending = false
		result.Message = &updated
		return nil
	})
	if err != nil {
		return StatusResult{}, err
	}
	if result.Changed && result.Message != nil {
		s.publish(realtime.Event{Type: "message.status", LocationID: result.Message.LocationID, Data: result.Message})
	}
	return result, nil
}

// ListConversations returns a page of conversations for a location.
func (s *Service) ListConversations(ctx context.Context, locationID string, f ConversationFilter) (Page[model.Conversation], error) {
	limit, err := normalizeLimit(f.Limit)
	if err != nil {
		return Page[model.Conversation]{}, err
	}
	if f.Status != "" && f.Status != model.ConversationOpen && f.Status != model.ConversationClosed {
		return Page[model.Conversation]{}, apperr.Invalid("status must be open or closed")
	}
	cursorAt, cursorID, err := cursor.Decode(f.Cursor)
	if err != nil {
		return Page[model.Conversation]{}, apperr.Invalid("cursor")
	}
	rows, err := s.store.ListConversations(ctx, locationID, f.Status, f.AssignedTo, cursorAt, cursorID, limit+1)
	if err != nil {
		return Page[model.Conversation]{}, err
	}
	page := Page[model.Conversation]{Data: rows}
	if len(rows) > limit {
		last := rows[limit-1]
		page.Data = rows[:limit]
		page.NextCursor = cursor.Encode(last.LastMessageAt, last.ID)
	}
	if page.Data == nil {
		page.Data = []model.Conversation{}
	}
	return page, nil
}

// ListMessages returns a chronological page of messages.
func (s *Service) ListMessages(ctx context.Context, conversationID, rawCursor string, limit int) (Page[model.Message], error) {
	if _, err := s.store.GetConversation(ctx, conversationID); err != nil {
		return Page[model.Message]{}, err
	}
	n, err := normalizeLimit(limit)
	if err != nil {
		return Page[model.Message]{}, err
	}
	cursorAt, cursorID, err := cursor.Decode(rawCursor)
	if err != nil {
		return Page[model.Message]{}, apperr.Invalid("cursor")
	}
	rows, err := s.store.ListMessages(ctx, conversationID, cursorAt, cursorID, n+1)
	if err != nil {
		return Page[model.Message]{}, err
	}
	page := Page[model.Message]{Data: rows}
	if len(rows) > n {
		last := rows[n-1]
		page.Data = rows[:n]
		page.NextCursor = cursor.Encode(last.SentAt, last.ID)
	}
	if page.Data == nil {
		page.Data = []model.Message{}
	}
	return page, nil
}

// MarkRead resets the unread counter.
func (s *Service) MarkRead(ctx context.Context, conversationID string) (model.Conversation, error) {
	if err := s.store.MarkRead(ctx, conversationID); err != nil {
		return model.Conversation{}, err
	}
	convo, err := s.store.GetConversation(ctx, conversationID)
	if err != nil {
		return model.Conversation{}, err
	}
	s.publish(realtime.Event{Type: "conversation.updated", LocationID: convo.LocationID, Data: convo})
	return convo, nil
}

// Patch assigns an agent and/or opens or closes the conversation.
func (s *Service) Patch(ctx context.Context, conversationID string, p PatchInput) (model.Conversation, error) {
	if err := p.validate(); err != nil {
		return model.Conversation{}, err
	}
	if err := s.store.PatchConversation(ctx, conversationID, p.Status, p.AssignedTo, p.AssignedTo != nil); err != nil {
		return model.Conversation{}, err
	}
	convo, err := s.store.GetConversation(ctx, conversationID)
	if err != nil {
		return model.Conversation{}, err
	}
	s.publish(realtime.Event{Type: "conversation.updated", LocationID: convo.LocationID, Data: convo})
	return convo, nil
}

// Search returns messages in a location whose body contains q, newest first.
func (s *Service) Search(ctx context.Context, locationID, q, rawCursor string, limit int) (Page[model.Message], error) {
	q = strings.TrimSpace(q)
	if q == "" {
		return Page[model.Message]{}, apperr.Invalid("q is required")
	}
	if len([]rune(q)) > 200 {
		return Page[model.Message]{}, apperr.Invalid("q is too long")
	}
	n, err := normalizeLimit(limit)
	if err != nil {
		return Page[model.Message]{}, err
	}
	cursorAt, cursorID, err := cursor.Decode(rawCursor)
	if err != nil {
		return Page[model.Message]{}, apperr.Invalid("cursor")
	}
	rows, err := s.store.SearchMessages(ctx, locationID, likeContains(q), cursorAt, cursorID, n+1)
	if err != nil {
		return Page[model.Message]{}, err
	}
	page := Page[model.Message]{Data: rows}
	if len(rows) > n {
		last := rows[n-1]
		page.Data = rows[:n]
		page.NextCursor = cursor.Encode(last.SentAt, last.ID)
	}
	if page.Data == nil {
		page.Data = []model.Message{}
	}
	return page, nil
}

// Typing broadcasts an ephemeral typing indicator to the conversation's location.
func (s *Service) Typing(ctx context.Context, conversationID, agentID string, typing bool) error {
	agentID = strings.TrimSpace(agentID)
	if agentID == "" {
		return apperr.Invalid("agentId is required")
	}
	convo, err := s.store.GetConversation(ctx, conversationID)
	if err != nil {
		return err
	}
	s.publish(realtime.Event{
		Type:       "typing",
		LocationID: convo.LocationID,
		Data: map[string]any{
			"conversationId": convo.ID,
			"agentId":        agentID,
			"typing":         typing,
		},
	})
	return nil
}

func (s *Service) publish(ev realtime.Event) {
	if s.hub != nil {
		s.hub.Publish(ev)
	}
}

func (s *Service) deliver(ctx context.Context, messageID string) {
	for {
		msg, err := s.store.GetMessage(ctx, messageID)
		if err != nil {
			log.Printf("deliver %s: load: %v", messageID, err)
			return
		}
		if msg.Status != status.Queued {
			return
		}
		if msg.AttemptCount >= 3 {
			s.finish(ctx, msg, "", status.Failed, msg.AttemptCount)
			return
		}
		providerID, sendErr := s.sender.Send(ctx, provider.Outbound{
			MessageID:      msg.ID,
			ConversationID: msg.ConversationID,
			LocationID:     msg.LocationID,
			Channel:        msg.Channel,
			From:           msg.From,
			To:             msg.To,
			Body:           msg.Body,
		})
		nextAttempt := msg.AttemptCount + 1
		if sendErr == nil {
			s.finish(ctx, msg, providerID, status.Sent, nextAttempt)
			return
		}
		if err := s.store.RecordAttempt(ctx, messageID, nextAttempt); err != nil {
			log.Printf("deliver %s: attempt: %v", messageID, err)
			return
		}
		if nextAttempt >= 3 {
			s.finish(ctx, msg, "", status.Failed, nextAttempt)
			return
		}
		delay := s.cfg.RetryBase * time.Duration(1<<(nextAttempt-1))
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (s *Service) finish(ctx context.Context, msg model.Message, providerID, proposed string, attempt int) {
	now := timefmt.Format(time.Now())
	var updated model.Message
	var changed bool
	err := s.store.WithTx(ctx, func(tx *store.Tx) error {
		current, err := tx.GetMessage(ctx, msg.ID)
		if err != nil {
			return err
		}
		if providerID != "" {
			if err := tx.SetProviderID(ctx, msg.ID, providerID); err != nil {
				return err
			}
		}
		next, ch := status.Advance(current.Status, proposed)
		changed = ch
		if providerID != "" {
			pending, err := tx.PendingStatus(ctx, providerID)
			if err != nil {
				return err
			}
			if pending != "" {
				var ch2 bool
				next, ch2 = status.Advance(next, pending)
				changed = changed || ch2
			}
			if err := tx.DeletePending(ctx, providerID); err != nil {
				return err
			}
		}
		if err := tx.UpdateDelivery(ctx, msg.ID, next, attempt, now); err != nil {
			return err
		}
		updated, err = tx.GetMessage(ctx, msg.ID)
		return err
	})
	if err != nil {
		log.Printf("deliver %s: finish: %v", msg.ID, err)
		return
	}
	if changed {
		s.publish(realtime.Event{Type: "message.status", LocationID: updated.LocationID, Data: updated})
	}
}

func mergePending(current, next string) string {
	merged, _ := status.Advance(current, next)
	if merged == current {
		// Advance refuses equals and backwards moves. If current cannot accept
		// next, keep current. If next is the first failed/queued-style value
		// that Advance accepts, merged is next.
		return current
	}
	return merged
}

func businessAddress(ctx context.Context, st *store.Store, conversationID string) string {
	last, err := st.LatestMessage(ctx, conversationID)
	if err != nil {
		return ""
	}
	if last.Direction == model.DirectionInbound {
		return last.To
	}
	return last.From
}

func identityFor(channel, from string) (string, error) {
	switch channel {
	case model.ChannelSMS, model.ChannelWhatsApp:
		phone := normalize.Phone(from)
		if !normalize.ValidPhone(phone) {
			return "", apperr.Invalid("from is not a valid phone number")
		}
		return "phone:" + phone, nil
	case model.ChannelEmail:
		email := normalize.Email(from)
		if !normalize.ValidEmail(email) {
			return "", apperr.Invalid("from is not a valid email")
		}
		return "email:" + email, nil
	default:
		return "", apperr.Invalid("channel must be sms, whatsapp, or email")
	}
}

func channelCompatible(kind, channel string) bool {
	switch kind {
	case "phone":
		return channel == model.ChannelSMS || channel == model.ChannelWhatsApp
	case "email":
		return channel == model.ChannelEmail
	default:
		return false
	}
}

func preview(body string) string {
	rs := []rune(body)
	if len(rs) <= previewRunes {
		return body
	}
	return string(rs[:previewRunes]) + "…"
}

func normalizeLimit(n int) (int, error) {
	if n == 0 {
		return defaultLimit, nil
	}
	if n < 1 || n > maxLimit {
		return 0, apperr.Invalid("limit must be between 1 and 100")
	}
	return n, nil
}

func likeContains(q string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return "%" + replacer.Replace(q) + "%"
}

var errDuplicate = errors.New("duplicate provider message")
