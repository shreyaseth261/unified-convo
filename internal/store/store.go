package store

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"path/filepath"
	"strings"
	"time"

	"unifiedconvo/internal/apperr"
	"unifiedconvo/internal/model"
	"unifiedconvo/internal/timefmt"

	_ "modernc.org/sqlite"
)

// Store is a SQLite persistence layer.
// Write transactions use BEGIN IMMEDIATE so check-then-insert stays atomic
// on SQLite's single-writer lock.
type Store struct {
	db *sql.DB
}

// DBTX is the subset of *sql.DB, *sql.Conn, and *sql.Tx used by queries.
type DBTX interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

// Tx is a write transaction bound to one connection.
type Tx struct {
	q DBTX
}

const schema = `
CREATE TABLE IF NOT EXISTS contacts (
	id TEXT PRIMARY KEY,
	location_id TEXT NOT NULL,
	identity TEXT NOT NULL,
	display TEXT NOT NULL,
	created_at TEXT NOT NULL,
	UNIQUE(location_id, identity)
);

CREATE TABLE IF NOT EXISTS conversations (
	id TEXT PRIMARY KEY,
	location_id TEXT NOT NULL,
	contact_id TEXT NOT NULL REFERENCES contacts(id),
	status TEXT NOT NULL,
	assigned_to TEXT,
	last_message_at TEXT NOT NULL,
	last_message_preview TEXT NOT NULL,
	unread_count INTEGER NOT NULL,
	created_at TEXT NOT NULL,
	updated_at TEXT NOT NULL,
	UNIQUE(location_id, contact_id)
);

CREATE INDEX IF NOT EXISTS idx_conversations_inbox
	ON conversations(location_id, status, last_message_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_conversations_assignee
	ON conversations(location_id, assigned_to, last_message_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS messages (
	id TEXT PRIMARY KEY,
	conversation_id TEXT NOT NULL REFERENCES conversations(id),
	location_id TEXT NOT NULL,
	direction TEXT NOT NULL,
	channel TEXT NOT NULL,
	body TEXT NOT NULL,
	from_addr TEXT NOT NULL,
	to_addr TEXT NOT NULL,
	provider_message_id TEXT,
	status TEXT NOT NULL,
	attempt_count INTEGER NOT NULL DEFAULT 0,
	sent_at TEXT NOT NULL,
	created_at TEXT NOT NULL,
	status_updated_at TEXT NOT NULL,
	UNIQUE(provider_message_id)
);

CREATE INDEX IF NOT EXISTS idx_messages_page
	ON messages(conversation_id, sent_at, id);

CREATE INDEX IF NOT EXISTS idx_messages_search
	ON messages(location_id, sent_at DESC, id DESC);

CREATE INDEX IF NOT EXISTS idx_messages_queued
	ON messages(direction, status);

CREATE TABLE IF NOT EXISTS pending_statuses (
	provider_message_id TEXT PRIMARY KEY,
	status TEXT NOT NULL,
	event_time TEXT NOT NULL
);
`

// Open creates the database file and schema.
func Open(path string) (*Store, error) {
	fileDSN, err := dsn(path)
	if err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", fileDSN)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)

	var fk int
	if err := db.QueryRow("PRAGMA foreign_keys").Scan(&fk); err != nil {
		_ = db.Close()
		return nil, err
	}
	if fk != 1 {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite foreign_keys pragma is %d", fk)
	}
	if _, err := db.Exec(schema); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

func dsn(path string) (string, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	// A relative path becomes the URI host (file://inbox.db), which SQLite rejects.
	// An absolute path with an empty host is file:///Users/.../inbox.db.
	u := url.URL{Scheme: "file", Path: filepath.ToSlash(abs)}
	q := url.Values{}
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "foreign_keys(1)")
	q.Add("_pragma", "journal_mode(WAL)")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// Close releases the pool.
func (s *Store) Close() error {
	return s.db.Close()
}

// WithTx runs fn inside BEGIN IMMEDIATE. The connection is not shared.
func (s *Store) WithTx(ctx context.Context, fn func(*Tx) error) error {
	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		}
	}()
	if err := fn(&Tx{q: conn}); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return err
	}
	committed = true
	return nil
}

// MessageByProviderID loads a message by the provider's idempotency key.
func (s *Store) MessageByProviderID(ctx context.Context, providerID string) (model.Message, error) {
	return messageByProviderID(ctx, s.db, providerID)
}

func (t *Tx) MessageByProviderID(ctx context.Context, providerID string) (model.Message, error) {
	return messageByProviderID(ctx, t.q, providerID)
}

func messageByProviderID(ctx context.Context, q DBTX, providerID string) (model.Message, error) {
	row := q.QueryRowContext(ctx, messageSelect+` WHERE provider_message_id = ?`, providerID)
	msg, err := scanMessage(row)
	if err != nil {
		return model.Message{}, err
	}
	return msg, nil
}

// GetMessage loads a message by primary key.
func (s *Store) GetMessage(ctx context.Context, id string) (model.Message, error) {
	return getMessage(ctx, s.db, id)
}

func (t *Tx) GetMessage(ctx context.Context, id string) (model.Message, error) {
	return getMessage(ctx, t.q, id)
}

func getMessage(ctx context.Context, q DBTX, id string) (model.Message, error) {
	row := q.QueryRowContext(ctx, messageSelect+` WHERE id = ?`, id)
	return scanMessage(row)
}

const messageSelect = `
SELECT id, conversation_id, location_id, direction, channel, body,
	from_addr, to_addr, provider_message_id, status, attempt_count,
	sent_at, created_at, status_updated_at
FROM messages`

type scanner interface {
	Scan(...any) error
}

func scanMessage(sc scanner) (model.Message, error) {
	var m model.Message
	var provider sql.NullString
	err := sc.Scan(
		&m.ID, &m.ConversationID, &m.LocationID, &m.Direction, &m.Channel, &m.Body,
		&m.From, &m.To, &provider, &m.Status, &m.AttemptCount,
		&m.SentAt, &m.CreatedAt, &m.StatusUpdatedAt,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return model.Message{}, apperr.ErrNotFound
		}
		return model.Message{}, err
	}
	if provider.Valid {
		m.ProviderMessageID = provider.String
	}
	return m, nil
}

// InsertMessage inserts a new row. A duplicate provider id returns ErrInvalid-unrelated unique via IsUnique.
func (t *Tx) InsertMessage(ctx context.Context, m model.Message) error {
	var provider any
	if m.ProviderMessageID != "" {
		provider = m.ProviderMessageID
	}
	_, err := t.q.ExecContext(ctx, `
		INSERT INTO messages (
			id, conversation_id, location_id, direction, channel, body,
			from_addr, to_addr, provider_message_id, status, attempt_count,
			sent_at, created_at, status_updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		m.ID, m.ConversationID, m.LocationID, m.Direction, m.Channel, m.Body,
		m.From, m.To, provider, m.Status, m.AttemptCount,
		m.SentAt, m.CreatedAt, m.StatusUpdatedAt,
	)
	return err
}

// IsUnique reports a SQLite unique-constraint failure.
func IsUnique(err error) bool {
	return err != nil && strings.Contains(err.Error(), "UNIQUE constraint failed")
}

// FindOrCreateContact returns the contact for a location and identity.
func (t *Tx) FindOrCreateContact(ctx context.Context, locationID, identity, display, id, now string) (model.Contact, error) {
	_, err := t.q.ExecContext(ctx, `
		INSERT INTO contacts (id, location_id, identity, display, created_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(location_id, identity) DO NOTHING`,
		id, locationID, identity, display, now,
	)
	if err != nil {
		return model.Contact{}, err
	}
	row := t.q.QueryRowContext(ctx, `
		SELECT id, identity, display FROM contacts WHERE location_id = ? AND identity = ?`,
		locationID, identity,
	)
	var c model.Contact
	if err := row.Scan(&c.ID, &c.Identity, &c.Display); err != nil {
		return model.Contact{}, err
	}
	fillContact(&c)
	return c, nil
}

func fillContact(c *model.Contact) {
	kind, value, _ := strings.Cut(c.Identity, ":")
	c.Kind = kind
	c.Value = value
}

// FindOrCreateConversation returns the single conversation for a contact at a location.
func (t *Tx) FindOrCreateConversation(ctx context.Context, locationID, contactID, id, sentAt, now string) (string, error) {
	_, err := t.q.ExecContext(ctx, `
		INSERT INTO conversations (
			id, location_id, contact_id, status, last_message_at, last_message_preview,
			unread_count, created_at, updated_at
		) VALUES (?, ?, ?, 'open', ?, '', 0, ?, ?)
		ON CONFLICT(location_id, contact_id) DO NOTHING`,
		id, locationID, contactID, sentAt, now, now,
	)
	if err != nil {
		return "", err
	}
	var existing string
	err = t.q.QueryRowContext(ctx, `
		SELECT id FROM conversations WHERE location_id = ? AND contact_id = ?`,
		locationID, contactID,
	).Scan(&existing)
	return existing, err
}

// TouchConversation applies unread, reopen, and last-message fields.
// last-message fields move forward only. Callers hold the write transaction,
// so the read-modify-write is serialized with other writers.
func (t *Tx) TouchConversation(ctx context.Context, id, sentAt, preview string, unreadDelta int, reopen bool) error {
	var status, lastAt, lastPreview string
	var unread int
	err := t.q.QueryRowContext(ctx, `
		SELECT status, last_message_at, last_message_preview, unread_count
		FROM conversations WHERE id = ?`, id,
	).Scan(&status, &lastAt, &lastPreview, &unread)
	if err != nil {
		if err == sql.ErrNoRows {
			return apperr.ErrNotFound
		}
		return err
	}
	if sentAt >= lastAt {
		lastAt = sentAt
		lastPreview = preview
	}
	if reopen {
		status = model.ConversationOpen
	}
	_, err = t.q.ExecContext(ctx, `
		UPDATE conversations
		SET status = ?, unread_count = ?, last_message_at = ?, last_message_preview = ?, updated_at = ?
		WHERE id = ?`,
		status, unread+unreadDelta, lastAt, lastPreview, timefmt.Format(time.Now()), id,
	)
	return err
}

// GetConversation loads a conversation and its contact.
func (s *Store) GetConversation(ctx context.Context, id string) (model.Conversation, error) {
	return getConversation(ctx, s.db, id)
}

func (t *Tx) GetConversation(ctx context.Context, id string) (model.Conversation, error) {
	return getConversation(ctx, t.q, id)
}

func getConversation(ctx context.Context, q DBTX, id string) (model.Conversation, error) {
	row := q.QueryRowContext(ctx, conversationSelect+` WHERE c.id = ?`, id)
	c, err := scanConversation(row)
	if err != nil {
		return model.Conversation{}, err
	}
	return c, nil
}

const conversationSelect = `
SELECT c.id, c.location_id, c.status, c.assigned_to, c.last_message_at, c.last_message_preview,
	c.unread_count, c.created_at, c.updated_at,
	ct.id, ct.identity, ct.display
FROM conversations c
JOIN contacts ct ON ct.id = c.contact_id`

func scanConversation(sc scanner) (model.Conversation, error) {
	var c model.Conversation
	var assigned sql.NullString
	err := sc.Scan(
		&c.ID, &c.LocationID, &c.Status, &assigned, &c.LastMessageAt, &c.LastMessagePreview,
		&c.UnreadCount, &c.CreatedAt, &c.UpdatedAt,
		&c.Contact.ID, &c.Contact.Identity, &c.Contact.Display,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return model.Conversation{}, apperr.ErrNotFound
		}
		return model.Conversation{}, err
	}
	if assigned.Valid {
		v := assigned.String
		c.AssignedTo = &v
	}
	fillContact(&c.Contact)
	return c, nil
}

// ListConversations returns up to limit rows ordered by latest message.
func (s *Store) ListConversations(ctx context.Context, locationID, status, assignedTo, cursorAt, cursorID string, limit int) ([]model.Conversation, error) {
	query := conversationSelect + ` WHERE c.location_id = ?`
	args := []any{locationID}
	if status != "" {
		query += ` AND c.status = ?`
		args = append(args, status)
	}
	if assignedTo != "" {
		query += ` AND c.assigned_to = ?`
		args = append(args, assignedTo)
	}
	if cursorAt != "" {
		query += ` AND (c.last_message_at < ? OR (c.last_message_at = ? AND c.id < ?))`
		args = append(args, cursorAt, cursorAt, cursorID)
	}
	query += ` ORDER BY c.last_message_at DESC, c.id DESC LIMIT ?`
	args = append(args, limit)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Conversation
	for rows.Next() {
		c, err := scanConversation(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// ListMessages returns up to limit messages in chronological order.
func (s *Store) ListMessages(ctx context.Context, conversationID, cursorAt, cursorID string, limit int) ([]model.Message, error) {
	query := messageSelect + ` WHERE conversation_id = ?`
	args := []any{conversationID}
	if cursorAt != "" {
		query += ` AND (sent_at > ? OR (sent_at = ? AND id > ?))`
		args = append(args, cursorAt, cursorAt, cursorID)
	}
	query += ` ORDER BY sent_at ASC, id ASC LIMIT ?`
	args = append(args, limit)
	return queryMessages(ctx, s.db, query, args...)
}

// SearchMessages finds messages in a location whose body contains q.
// q must already be escaped for LIKE.
func (s *Store) SearchMessages(ctx context.Context, locationID, like, cursorAt, cursorID string, limit int) ([]model.Message, error) {
	query := messageSelect + ` WHERE location_id = ? AND body LIKE ? ESCAPE '\'`
	args := []any{locationID, like}
	if cursorAt != "" {
		query += ` AND (sent_at < ? OR (sent_at = ? AND id < ?))`
		args = append(args, cursorAt, cursorAt, cursorID)
	}
	query += ` ORDER BY sent_at DESC, id DESC LIMIT ?`
	args = append(args, limit)
	return queryMessages(ctx, s.db, query, args...)
}

func queryMessages(ctx context.Context, q DBTX, query string, args ...any) ([]model.Message, error) {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []model.Message
	for rows.Next() {
		m, err := scanMessage(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// LatestMessage returns the newest message on a conversation.
func (s *Store) LatestMessage(ctx context.Context, conversationID string) (model.Message, error) {
	row := s.db.QueryRowContext(ctx, messageSelect+`
		WHERE conversation_id = ?
		ORDER BY sent_at DESC, id DESC LIMIT 1`, conversationID)
	return scanMessage(row)
}

// MarkRead sets unread_count to 0.
func (s *Store) MarkRead(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `
		UPDATE conversations SET unread_count = 0, updated_at = ? WHERE id = ?`,
		timefmt.Format(time.Now()), id,
	)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return apperr.ErrNotFound
	}
	return nil
}

// PatchConversation updates status and assignee when the matching flag is set.
// An empty assignee clears the assignment.
func (s *Store) PatchConversation(ctx context.Context, id string, status *string, assigned *string, assignedSet bool) error {
	current, err := s.GetConversation(ctx, id)
	if err != nil {
		return err
	}
	nextStatus := current.Status
	if status != nil {
		nextStatus = *status
	}
	var assignedParam any
	if assignedSet {
		if assigned != nil && *assigned != "" {
			assignedParam = *assigned
		}
	} else if current.AssignedTo != nil {
		assignedParam = *current.AssignedTo
	}
	res, err := s.db.ExecContext(ctx, `
		UPDATE conversations SET status = ?, assigned_to = ?, updated_at = ? WHERE id = ?`,
		nextStatus, assignedParam, timefmt.Format(time.Now()), id,
	)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return apperr.ErrNotFound
	}
	return nil
}

// RecordAttempt stores a failed send attempt while the message is still queued.
func (s *Store) RecordAttempt(ctx context.Context, id string, attempt int) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE messages SET attempt_count = ? WHERE id = ? AND status = 'queued'`,
		attempt, id,
	)
	return err
}

// QueuedOutboundIDs returns messages that still need a send attempt.
func (s *Store) QueuedOutboundIDs(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id FROM messages
		WHERE direction = 'outbound' AND status = 'queued' AND attempt_count < 3
		ORDER BY created_at ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// SetProviderID attaches the provider id if it is unset or already this value.
func (t *Tx) SetProviderID(ctx context.Context, messageID, providerID string) error {
	res, err := t.q.ExecContext(ctx, `
		UPDATE messages
		SET provider_message_id = ?
		WHERE id = ? AND (provider_message_id IS NULL OR provider_message_id = ?)`,
		providerID, messageID, providerID,
	)
	if err != nil {
		return err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 1 {
		return nil
	}
	msg, err := t.GetMessage(ctx, messageID)
	if err != nil {
		return err
	}
	if msg.ProviderMessageID == providerID {
		return nil
	}
	return fmt.Errorf("provider message id already set to %s", msg.ProviderMessageID)
}

// PendingStatus returns a stored early callback, if any.
func (t *Tx) PendingStatus(ctx context.Context, providerID string) (string, error) {
	var st string
	err := t.q.QueryRowContext(ctx, `
		SELECT status FROM pending_statuses WHERE provider_message_id = ?`, providerID,
	).Scan(&st)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return st, err
}

// UpsertPending keeps the furthest-advanced early status for a provider id.
func (t *Tx) UpsertPending(ctx context.Context, providerID, st, eventTime string, keep func(current, next string) string) error {
	current, err := t.PendingStatus(ctx, providerID)
	if err != nil {
		return err
	}
	chosen := st
	if current != "" {
		chosen = keep(current, st)
	}
	_, err = t.q.ExecContext(ctx, `
		INSERT INTO pending_statuses (provider_message_id, status, event_time)
		VALUES (?, ?, ?)
		ON CONFLICT(provider_message_id) DO UPDATE SET status = excluded.status, event_time = excluded.event_time`,
		providerID, chosen, eventTime,
	)
	return err
}

// DeletePending removes a consumed early callback.
func (t *Tx) DeletePending(ctx context.Context, providerID string) error {
	_, err := t.q.ExecContext(ctx, `DELETE FROM pending_statuses WHERE provider_message_id = ?`, providerID)
	return err
}

// UpdateDelivery writes the resolved status and attempt count.
func (t *Tx) UpdateDelivery(ctx context.Context, messageID, st string, attempt int, now string) error {
	_, err := t.q.ExecContext(ctx, `
		UPDATE messages
		SET status = ?, attempt_count = ?, status_updated_at = ?
		WHERE id = ?`,
		st, attempt, now, messageID,
	)
	return err
}

// ApplyDirectStatus advances a message that already has this provider id.
func (t *Tx) ApplyDirectStatus(ctx context.Context, messageID, st, now string) error {
	_, err := t.q.ExecContext(ctx, `
		UPDATE messages SET status = ?, status_updated_at = ? WHERE id = ?`,
		st, now, messageID,
	)
	return err
}
