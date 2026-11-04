# Unified Conversations Inbox

Go service. SQLite. One inbox per location. SMS, WhatsApp, and email are mocked.

## Requirements

1. `POST /webhooks/inbound` for SMS, WhatsApp, and email.
2. Find or create a contact by normalized phone (SMS and WhatsApp) or email, scoped to `locationId`.
3. One conversation per contact per location. Every channel for that contact shares it.
4. The same `providerMessageId` is stored once.
5. Update `lastMessageAt`, `lastMessagePreview`, and `unreadCount`.
6. `POST /conversations/:id/messages` saves `queued` and sends asynchronously through a mock provider.
7. The mock fails about 20% of the time. Retry with backoff, 3 attempts, then `failed`.
8. `POST /webhooks/status`. Lifecycle `queued → sent → delivered → read`. `failed` can happen, and status never moves backwards.
9. A status callback may arrive before the outbound row is saved.
10. List conversations by latest message, with `status` and `assignedTo` filters and cursor pagination.
11. List messages chronologically with cursor pagination.
12. Mark a conversation read. Assign it, or open and close it.
13. A new inbound message reopens a closed conversation.
14. Pages must not duplicate or skip rows when new messages arrive between requests.
15. Push new messages and status changes over SSE, only for that location.
16. Tests for duplicate webhooks, out-of-order status, 50 parallel inbound messages (`unreadCount == 50`), and pagination under inserts.
17. Message search and typing events.

## Assumptions

- No authentication. `locationId` is the tenant boundary.
- Locations are external ids. There is no locations table. An unknown location is an empty inbox, not a 404.
- SMS and WhatsApp with the same normalized phone are one contact. Email is a separate contact.
- Phone normalization strips formatting and keeps a leading `+`. Email is trimmed and lowercased.
- Inbound messages are stored as `delivered` because they have already reached the inbox. Unread is a conversation counter, not a per-message agent read receipt.
- Outbound `to` is the contact's canonical address. `from` is the business address taken from the latest inbound `to` when we have one.
- An outbound channel must match the contact kind.
- Outbound updates the preview and does not change `unreadCount`.
- `lastMessageAt` only moves forward. An older `sentAt` still increments unread and can reopen the thread.
- `failed` may replace `queued` or `sent` only. It does not overwrite `delivered` or `read`, and nothing leaves `failed`.
- Empty `assignedTo` clears the assignee. Omitting the field leaves it alone.
- Body is required and capped at 10,000 runes. Page size defaults to 20 and maxes at 100.
- Status webhooks for an unknown provider id return 200 after the status is stored, so we do not depend on the provider retrying.
- SSE subscribers with a full buffer drop the event. They can resync from the list APIs.
- A conversation that jumps ahead of a descending cursor is delivered via `conversation.updated`, not by a later page.

## Design

```
HTTP (chi) → inbox service → SQLite store
                     ↓
              mock provider
                     ↓
              in-memory hub → SSE per location
```

Write transactions use `BEGIN IMMEDIATE` on a dedicated connection. That serializes the check-then-insert for provider ids and the read-modify-write of `unread_count`.

Outbound sends go through four workers and a buffered job channel. Attempt count is on the message row. On startup, queued rows with fewer than 3 attempts are enqueued again. Tests set `SyncSend` so delivery finishes before the HTTP response.

Status order lives in `internal/status`. The store does not invent a second set of rules.

## 10,000 inbound messages per second

What is running now is one process and one SQLite file. That is a good fit for building the service and running the tests. At 10,000 inbound messages a second I would stop asking the request handler to do the database work.

The handler would check the payload, append it to a log such as Kafka, and return. The log would be split by `locationId`, so all messages for one store stay in order. If Twilio retries the same webhook, the consumer still ignores it because `providerMessageId` is unique.

Workers would read that log and write Postgres, using the same tables we have now. The new message and the unread increment would still happen in one transaction, so a burst cannot lose counts. If one location is much busier than the others, that partition can be split again by contact. Search would move to Postgres full text, because a `LIKE '%offer%'` scan will not keep up.

Sending replies would be a separate pool of workers. Each one claims a queued row with `SELECT … FOR UPDATE SKIP LOCKED` before calling the provider, so two servers never send the same message. The attempt count stays on the row, the way it does today.

The live updates need a shared bus, Redis or NATS. When a message is saved, the worker publishes `message.created` or `message.status`. Every API server is subscribed. A server only forwards an event to the agents who are connected to it. Someone on server A and someone on server B both see the same thread, and it does not matter which server they opened.

## Code structure

```
cmd/server/main.go          wires the process
internal/httpapi/handler.go HTTP only: decode, status code, JSON
internal/inbox/service.go   rules
internal/inbox/input.go     request validation
internal/store/store.go     SQL
internal/provider/mock.go   20% send failure
internal/realtime/hub.go    per-location subscribers
internal/status/status.go   queued < sent < delivered < read, failed terminal
internal/normalize/normalize.go
internal/cursor/cursor.go
internal/id/id.go
internal/timefmt/timefmt.go
internal/model/model.go
internal/apperr/errors.go
```

`main.go` opens the SQLite file, builds the hub, the mock provider, and `inbox.Service` (which starts the workers), then passes the service and hub to `httpapi.New`. Every route is registered there. A handler never writes SQL.

### Inbound

`POST /webhooks/inbound`

1. `handler.go` `inbound` decodes JSON into `inbox.InboundInput`.
2. `service.go` `Inbound` calls `input.go` `validate`, then `identityFor`.
3. `identityFor` calls `normalize.Phone` or `normalize.Email` and returns `phone:+91…` or `email:…`.
4. `store.go` `WithTx` (`BEGIN IMMEDIATE`):
   - `MessageByProviderID`. A hit returns the existing row and stops.
   - `FindOrCreateContact` on `(location_id, identity)`.
   - `FindOrCreateConversation` on `(location_id, contact_id)`.
   - `InsertMessage`. A unique violation rolls back and the service returns the row the other request saved.
   - `TouchConversation` adds 1 to unread, sets status `open`, and moves preview forward only.
5. After commit, `service.go` calls `realtime.Hub.Publish` with `message.created` and `conversation.updated`.
6. `handler.go` writes `201`, or `200` with `"duplicate": true`.

`GET /locations/{locationId}/events` never goes through the service. `handler.go` `events` calls `hub.Subscribe(locationId)` and writes Server-Sent Events. `Publish` sends only to channels registered for that location id.

### Send

`POST /conversations/{id}/messages`

1. `handler.go` `send` decodes `channel` and `body`.
2. `service.go` `Send` loads the conversation from `store.go`. `channelCompatible` rejects SMS to an email contact.
3. `store.go` inserts the message as `queued` and updates the preview. Unread is unchanged. A closed thread stays closed.
4. `service.go` publishes `message.created` and `conversation.updated`, then puts the message id on the job channel.
5. `handler.go` returns `202` with status `queued`. The provider has not been called yet.
6. `service.go` `worker` → `deliver` calls `provider/mock.go` `Send`. About 20% of calls return an error.
7. Success: `finish` sets the `mock_…` provider id and advances status with `status.Advance`. Failure: wait, try again, then `failed` on the third attempt.
8. `finish` also reads `pending_statuses` for that provider id and keeps the further status.
9. A real status change publishes `message.status`.

### Status

`POST /webhooks/status`

1. `handler.go` `status` decodes the body. `input.go` allows `sent`, `delivered`, `read`, `failed`.
2. `service.go` `Status` runs in `store.go` `WithTx`.
3. No message for that provider id: `UpsertPending` stores the furthest status and the handler returns `"pending": true`.
4. Message found: `status.Advance` compares rank. A lower status is ignored. `failed` replaces only `queued` or `sent`.
5. On a change, `ApplyDirectStatus` writes the new status and `Hub.Publish` sends `message.status`.

### Lists, read, patch

`handler.go` parses `limit`, `cursor`, `status`, and `assignedTo`, then calls `service.go`.

`ListConversations` and `ListMessages` call `cursor.Decode`, then `store.go`. The query is the previous `(timestamp, id)` pair, not an offset. The next cursor is `cursor.Encode` of the last row.

`MarkRead` sets `unread_count` to 0. `Patch` sets assignee and open or closed. Both publish `conversation.updated`.

`Search` is the same list path with a `LIKE` on `body`. `Typing` loads the conversation only to learn its `locationId`, then publishes `typing`. Nothing is stored.
