# Unified Conversations Inbox

Go API for one inbox per location. SMS, WhatsApp, and email are mocked. SQLite is a file the server creates. Go 1.25 or newer.

## Setup

| Variable | Default | Meaning |
| --- | --- | --- |
| `PORT` | `8080` | HTTP port |
| `DB_PATH` | `inbox.db` | SQLite file in the working directory |
| `SEND_FAIL_RATE` | `0.2` | Chance a send is rejected |
| `RETRY_BASE_MS` | `200` | First retry wait. The next wait is twice that |

```bash
SEND_FAIL_RATE=0 PORT=8098 go run ./cmd/server
```

Stop with Ctrl-C. Delete `inbox.db` to start over.

```bash
go test ./...
```

Set `BASE` in the terminal you use for curls:

```bash
BASE=http://127.0.0.1:8098
```

## Events

Leave this running. It prints `: connected`, then events for `loc_1` only.

```bash
curl -N $BASE/locations/loc_1/events
```

A second stream for another location stays quiet while you post to `loc_1`:

```bash
curl -N $BASE/locations/loc_2/events
```

## Inbound

```bash
curl -sS -X POST $BASE/webhooks/inbound -H 'Content-Type: application/json' -d '{
  "providerMessageId": "SM_abc123",
  "locationId": "loc_1",
  "channel": "sms",
  "from": "+91 98765 43210",
  "to": "+91 80000 00000",
  "body": "Is the offer still valid?",
  "sentAt": "2026-10-28T10:15:00Z"
}'
```

Copy `conversation.id` into `CID`.

Same `providerMessageId` again. `200`, `"duplicate": true`, unread stays 1.

```bash
curl -sS -X POST $BASE/webhooks/inbound -H 'Content-Type: application/json' -d '{
  "providerMessageId": "SM_abc123",
  "locationId": "loc_1",
  "channel": "sms",
  "from": "+91 98765 43210",
  "to": "+91 80000 00000",
  "body": "Is the offer still valid?",
  "sentAt": "2026-10-28T10:15:00Z"
}'
```

Same phone on WhatsApp joins that conversation. Unread becomes 2.

```bash
curl -sS -X POST $BASE/webhooks/inbound -H 'Content-Type: application/json' -d '{
  "providerMessageId": "WA_1",
  "locationId": "loc_1",
  "channel": "whatsapp",
  "from": "+91-98765-43210",
  "to": "+91 80000 00000",
  "body": "checking again",
  "sentAt": "2026-10-28T10:16:00Z"
}'
```

An older `sentAt` increases unread and does not replace the preview.

```bash
curl -sS -X POST $BASE/webhooks/inbound -H 'Content-Type: application/json' -d '{
  "providerMessageId": "SM_old",
  "locationId": "loc_1",
  "channel": "sms",
  "from": "+91 98765 43210",
  "to": "+91 80000 00000",
  "body": "older-note",
  "sentAt": "2026-10-28T09:00:00Z"
}'
```

Same phone at another location is a different conversation.

```bash
curl -sS -X POST $BASE/webhooks/inbound -H 'Content-Type: application/json' -d '{
  "providerMessageId": "SM_loc2",
  "locationId": "loc_2",
  "channel": "sms",
  "from": "+91 98765 43210",
  "to": "+91 80000 00000",
  "body": "other store",
  "sentAt": "2026-10-28T10:20:00Z"
}'
```

Email is its own contact. Copy this `conversation.id` if you want `EID` below.

```bash
curl -sS -X POST $BASE/webhooks/inbound -H 'Content-Type: application/json' -d '{
  "providerMessageId": "EM_1",
  "locationId": "loc_1",
  "channel": "email",
  "from": "Customer@Example.com",
  "to": "inbox@example.com",
  "body": "Need help",
  "sentAt": "2026-10-28T10:21:00Z"
}'
```

Missing fields. `400`.

```bash
curl -sS -X POST $BASE/webhooks/inbound -H 'Content-Type: application/json' -d '{"channel":"pigeon"}'
```

## Send

```bash
CID=paste-the-sms-conversation-id

curl -sS -X POST $BASE/conversations/$CID/messages \
  -H 'Content-Type: application/json' \
  -d '{"channel":"whatsapp","body":"Yes, valid till Friday!"}'; echo
```

`202`, status `queued`. Fetch the thread and copy the outbound `providerMessageId` (`mock_…`) once status is `sent`.

```bash
curl -sS $BASE/conversations/$CID/messages; echo
```

SMS to an email contact. `400`.

```bash
EID=paste-the-email-conversation-id

curl -sS -X POST $BASE/conversations/$EID/messages \
  -H 'Content-Type: application/json' \
  -d '{"channel":"sms","body":"nope"}'
```

Unknown conversation. `404`.

```bash
curl -sS -X POST $BASE/conversations/missing/messages \
  -H 'Content-Type: application/json' \
  -d '{"channel":"sms","body":"hi"}'
```

To see a failed send, restart with `SEND_FAIL_RATE=1` and post another reply. After 3 attempts the message status is `failed`.

## Status

```bash
PID=paste-the-mock-id

curl -sS -X POST $BASE/webhooks/status -H 'Content-Type: application/json' -d "{
  \"providerMessageId\": \"$PID\",
  \"status\": \"delivered\",
  \"timestamp\": \"2026-10-28T10:16:00Z\"
}"; echo

curl -sS -X POST $BASE/webhooks/status -H 'Content-Type: application/json' -d "{
  \"providerMessageId\": \"$PID\",
  \"status\": \"sent\",
  \"timestamp\": \"2026-10-28T10:16:01Z\"
}"; echo
```

First response: `"changed": true`, status `delivered`. Second: `"changed": false`, still `delivered`.

Unknown provider id. `"pending": true`.

```bash
curl -sS -X POST $BASE/webhooks/status -H 'Content-Type: application/json' -d '{
  "providerMessageId": "does-not-exist",
  "status": "delivered",
  "timestamp": "2026-10-28T10:16:02Z"
}'
```

`read`, then `failed`. Stays `read`.

```bash
curl -sS -X POST $BASE/webhooks/status -H 'Content-Type: application/json' -d "{
  \"providerMessageId\": \"$PID\",
  \"status\": \"read\",
  \"timestamp\": \"2026-10-28T10:17:00Z\"
}"; echo

curl -sS -X POST $BASE/webhooks/status -H 'Content-Type: application/json' -d "{
  \"providerMessageId\": \"$PID\",
  \"status\": \"failed\",
  \"timestamp\": \"2026-10-28T10:18:00Z\"
}"; echo
```

## Inbox

```bash
curl -sS "$BASE/locations/loc_1/conversations?status=open"; echo
curl -sS "$BASE/locations/loc_1/conversations?limit=1"; echo
```

Pass `nextCursor` from that response:

```bash
curl -sS --get "$BASE/locations/loc_1/conversations" --data-urlencode "limit=1" --data-urlencode "cursor=PASTE"; echo

curl -sS -X POST $BASE/conversations/$CID/read; echo

curl -sS -X PATCH $BASE/conversations/$CID -H 'Content-Type: application/json' \
  -d '{"assignedTo":"agent_7","status":"closed"}'; echo

curl -sS "$BASE/locations/loc_1/conversations?status=closed&assignedTo=agent_7"; echo

curl -sS -X PATCH $BASE/conversations/$CID -H 'Content-Type: application/json' \
  -d '{"assignedTo":""}'; echo

curl -sS "$BASE/conversations/$CID/messages?limit=1"; echo
curl -sS --get "$BASE/conversations/$CID/messages" --data-urlencode "limit=1" --data-urlencode "cursor=PASTE"; echo

curl -sS --get $BASE/locations/loc_1/messages/search --data-urlencode 'q=offer'; echo
curl -sS --get $BASE/locations/loc_1/messages/search --data-urlencode 'q=nomatch'; echo

curl -sS -D - -o /dev/null -X POST $BASE/conversations/$CID/typing \
  -H 'Content-Type: application/json' \
  -d '{"agentId":"agent_7","typing":true}'
```

New inbound on the closed thread. Status returns to `open`, unread is 1. The events terminal prints `message.created`.

```bash
curl -sS -X POST $BASE/webhooks/inbound -H 'Content-Type: application/json' -d '{
  "providerMessageId": "SM_reopen",
  "locationId": "loc_1",
  "channel": "sms",
  "from": "+91 98765 43210",
  "to": "+91 80000 00000",
  "body": "still there?",
  "sentAt": "2026-10-28T12:00:00Z"
}'
```

```bash
curl -sS $BASE/healthz; echo
```
