package httpapi_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"unifiedconvo/internal/httpapi"
	"unifiedconvo/internal/inbox"
	"unifiedconvo/internal/model"
	"unifiedconvo/internal/provider"
	"unifiedconvo/internal/realtime"
	"unifiedconvo/internal/store"
)

func TestDuplicateInboundWebhook(t *testing.T) {
	t.Parallel()
	srv := newServer(t, nil)
	payload := inbound("+919876543210", "SM_abc123", "sms", "Is the offer still valid?", time.Date(2026, 10, 28, 10, 15, 0, 0, time.UTC))

	code, first := do(t, http.MethodPost, srv.URL+"/webhooks/inbound", payload)
	if code != http.StatusCreated {
		t.Fatalf("status %d body %s", code, first)
	}
	code, second := do(t, http.MethodPost, srv.URL+"/webhooks/inbound", payload)
	if code != http.StatusOK {
		t.Fatalf("duplicate status %d body %s", code, second)
	}
	a := decode[inboundResult](t, first)
	b := decode[inboundResult](t, second)
	if !b.Duplicate || a.Message.ID != b.Message.ID {
		t.Fatalf("duplicate was stored again: %#v %#v", a, b)
	}
	if a.Conversation.UnreadCount != 1 {
		t.Fatalf("unread = %d", a.Conversation.UnreadCount)
	}
	msgs := allMessages(t, srv, a.Conversation.ID)
	if len(msgs) != 1 {
		t.Fatalf("stored %d messages", len(msgs))
	}
}

func TestParallelDuplicateInbound(t *testing.T) {
	t.Parallel()
	srv := newServer(t, nil)
	payload := inbound("+15550000001", "SM_same", "sms", "hello", time.Now().UTC())
	var wg sync.WaitGroup
	codes := make(chan int, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			code, body, err := doRaw(http.MethodPost, srv.URL+"/webhooks/inbound", payload)
			if err != nil {
				t.Error(err)
				return
			}
			if code != http.StatusCreated && code != http.StatusOK {
				t.Errorf("status %d body %s", code, body)
			}
			codes <- code
		}()
	}
	wg.Wait()
	close(codes)
	created := 0
	for code := range codes {
		if code == http.StatusCreated {
			created++
		}
	}
	if created != 1 {
		t.Fatalf("created %d times", created)
	}
	page := listConversations(t, srv, "loc_1", "")
	if len(page.Data) != 1 || page.Data[0].UnreadCount != 1 {
		t.Fatalf("inbox %#v", page.Data)
	}
}

func TestParallelInboundUnreadCount(t *testing.T) {
	t.Parallel()
	srv := newServer(t, nil)
	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payload := inbound("+15550000002", fmt.Sprintf("SM_%02d", i), "sms", fmt.Sprintf("m%d", i), time.Now().UTC().Add(time.Duration(i)*time.Millisecond))
			code, body, err := doRaw(http.MethodPost, srv.URL+"/webhooks/inbound", payload)
			if err != nil {
				t.Error(err)
				return
			}
			if code != http.StatusCreated {
				t.Errorf("status %d body %s", code, body)
			}
		}(i)
	}
	wg.Wait()
	if t.Failed() {
		return
	}
	page := listConversations(t, srv, "loc_1", "")
	if len(page.Data) != 1 {
		t.Fatalf("conversations = %d", len(page.Data))
	}
	if page.Data[0].UnreadCount != n {
		t.Fatalf("unreadCount = %d", page.Data[0].UnreadCount)
	}
	if got := len(allMessages(t, srv, page.Data[0].ID)); got != n {
		t.Fatalf("messages = %d", got)
	}
}

func TestOutOfOrderStatus(t *testing.T) {
	t.Parallel()
	srv := newServer(t, &scripted{})
	convo := postInbound(t, srv, inbound("+15550000003", "SM_seed", "whatsapp", "hi", time.Now().UTC()))
	sent := postSend(t, srv, convo.ID, "whatsapp", "Yes, valid till Friday!")
	if sent.Message.Status != "sent" || sent.Message.ProviderMessageID == "" {
		t.Fatalf("send result %#v", sent.Message)
	}
	postStatus(t, srv, sent.Message.ProviderMessageID, "delivered")
	postStatus(t, srv, sent.Message.ProviderMessageID, "sent")
	msg := mustMessage(t, srv, convo.ID, sent.Message.ID)
	if msg.Status != "delivered" {
		t.Fatalf("status moved backwards to %s", msg.Status)
	}
	postStatus(t, srv, sent.Message.ProviderMessageID, "read")
	postStatus(t, srv, sent.Message.ProviderMessageID, "failed")
	msg = mustMessage(t, srv, convo.ID, sent.Message.ID)
	if msg.Status != "read" {
		t.Fatalf("status = %s, want read", msg.Status)
	}
}

func TestStatusArrivesBeforeProviderIDPersisted(t *testing.T) {
	t.Parallel()
	var (
		base    string
		hookErr error
	)
	sender := &scripted{before: func(providerID string) {
		code, body, err := doRaw(http.MethodPost, base+"/webhooks/status", map[string]any{
			"providerMessageId": providerID,
			"status":            "delivered",
			"timestamp":         time.Now().UTC().Format(time.RFC3339),
		})
		if err != nil || code != http.StatusOK {
			hookErr = fmt.Errorf("early status %d %s err=%v", code, body, err)
		}
	}}
	srv := newServer(t, sender)
	base = srv.URL
	convo := postInbound(t, srv, inbound("+15550000004", "SM_early", "sms", "ping", time.Now().UTC()))
	sent := postSend(t, srv, convo.ID, "sms", "pong")
	if hookErr != nil {
		t.Fatal(hookErr)
	}
	if sent.Message.Status != "delivered" {
		t.Fatalf("status = %s, want delivered after early callback", sent.Message.Status)
	}
	if sent.Message.ProviderMessageID == "" {
		t.Fatal("missing provider id")
	}
}

func TestOutboundRetryThenSuccess(t *testing.T) {
	t.Parallel()
	srv := newServer(t, &scripted{failFirst: 2})
	convo := postInbound(t, srv, inbound("+15550000005", "SM_retry", "sms", "hi", time.Now().UTC()))
	sent := postSend(t, srv, convo.ID, "sms", "reply")
	if sent.Message.Status != "sent" || sent.Message.AttemptCount != 3 {
		t.Fatalf("message %#v", sent.Message)
	}
}

func TestOutboundFailsAfterThreeAttempts(t *testing.T) {
	t.Parallel()
	srv := newServer(t, &scripted{failFirst: 3})
	convo := postInbound(t, srv, inbound("+15550000006", "SM_fail", "sms", "hi", time.Now().UTC()))
	sent := postSend(t, srv, convo.ID, "sms", "reply")
	if sent.Message.Status != "failed" || sent.Message.AttemptCount != 3 {
		t.Fatalf("message %#v", sent.Message)
	}
}

func TestMessagePaginationStableWhenInserting(t *testing.T) {
	t.Parallel()
	srv := newServer(t, nil)
	base := time.Date(2026, 10, 28, 10, 0, 0, 0, time.UTC)
	var convoID string
	original := map[string]struct{}{}
	for i := 1; i <= 10; i++ {
		res := postInbound(t, srv, inbound("+15550000007", fmt.Sprintf("SM_page_%02d", i), "sms", fmt.Sprintf("n%d", i), base.Add(time.Duration(i)*time.Minute)))
		convoID = res.ID
		original[res.LastMessageID] = struct{}{}
	}
	first := getMessages(t, srv, convoID, "", 4)
	if len(first.Data) != 4 || first.NextCursor == nil {
		t.Fatalf("first page %#v", first)
	}
	// One lands inside the next page, one lands after everything already stored.
	mid := postInbound(t, srv, inbound("+15550000007", "SM_page_mid", "sms", "middle", base.Add(4*time.Minute+30*time.Second)))
	late := postInbound(t, srv, inbound("+15550000007", "SM_page_late", "sms", "late", base.Add(20*time.Minute)))

	seen := map[string]int{}
	for _, m := range first.Data {
		seen[m.ID]++
	}
	cursor := first.NextCursor
	for cursor != nil {
		page := getMessages(t, srv, convoID, *cursor, 4)
		for _, m := range page.Data {
			seen[m.ID]++
		}
		cursor = page.NextCursor
	}
	if len(seen) != 12 {
		t.Fatalf("saw %d distinct messages", len(seen))
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("message %s seen %d times", id, n)
		}
	}
	for id := range original {
		if seen[id] != 1 {
			t.Fatalf("original %s missing", id)
		}
	}
	if seen[mid.LastMessageID] != 1 || seen[late.LastMessageID] != 1 {
		t.Fatal("inserted messages were skipped or duplicated")
	}
	for _, m := range first.Data {
		if m.ID == mid.LastMessageID || m.ID == late.LastMessageID {
			t.Fatal("new messages appeared on the page that was already returned")
		}
	}
}

func TestConversationPaginationStableWhenInserting(t *testing.T) {
	t.Parallel()
	srv := newServer(t, nil)
	base := time.Date(2026, 10, 28, 9, 0, 0, 0, time.UTC)
	original := map[string]struct{}{}
	for i := 1; i <= 6; i++ {
		res := postInbound(t, srv, inbound(fmt.Sprintf("+15550001%03d", i), fmt.Sprintf("SM_c_%d", i), "sms", "hello", base.Add(time.Duration(i)*time.Minute)))
		original[res.ID] = struct{}{}
	}
	first := listConversations(t, srv, "loc_1", "limit=2")
	if len(first.Data) != 2 || first.NextCursor == nil {
		t.Fatalf("first page %#v", first.Data)
	}
	newest := postInbound(t, srv, inbound("+15550001999", "SM_c_new", "sms", "brand new", base.Add(time.Hour)))

	seen := map[string]int{}
	for _, c := range first.Data {
		seen[c.ID]++
	}
	cursor := first.NextCursor
	for cursor != nil {
		q := "limit=2&cursor=" + *cursor
		page := listConversations(t, srv, "loc_1", q)
		for _, c := range page.Data {
			seen[c.ID]++
			if c.ID == newest.ID {
				t.Fatal("new head conversation was duplicated onto a later page")
			}
		}
		cursor = page.NextCursor
	}
	if len(seen) != 6 {
		t.Fatalf("saw %d conversations, want the original 6", len(seen))
	}
	for id := range original {
		if seen[id] != 1 {
			t.Fatalf("conversation %s seen %d times", id, seen[id])
		}
	}
	if _, ok := seen[newest.ID]; ok {
		t.Fatal("conversation inserted ahead of the cursor was mixed into the walk")
	}
}

func TestPhoneNormalizationUnifiesChannels(t *testing.T) {
	t.Parallel()
	srv := newServer(t, nil)
	a := postInbound(t, srv, inbound("+91 98765 43210", "SM_fmt_1", "sms", "from sms", time.Now().UTC()))
	b := postInbound(t, srv, inbound("+91-98765-43210", "SM_fmt_2", "whatsapp", "from whatsapp", time.Now().UTC().Add(time.Second)))
	if a.ID != b.ID {
		t.Fatalf("created two conversations %s and %s", a.ID, b.ID)
	}
	if b.Unread != 2 {
		t.Fatalf("unread = %d", b.Unread)
	}
}

func TestOlderInboundDoesNotMovePreviewBackwards(t *testing.T) {
	t.Parallel()
	srv := newServer(t, nil)
	base := time.Date(2026, 10, 28, 12, 0, 0, 0, time.UTC)
	postInbound(t, srv, inbound("+15550000009", "SM_new", "sms", "newer-offer", base.Add(time.Hour)))
	res := postInbound(t, srv, inbound("+15550000009", "SM_old", "sms", "older-note", base))
	if res.Preview != "newer-offer" {
		t.Fatalf("preview = %q", res.Preview)
	}
	if res.Unread != 2 {
		t.Fatalf("unread = %d", res.Unread)
	}
	msgs := allMessages(t, srv, res.ID)
	if len(msgs) != 2 {
		t.Fatalf("messages = %d", len(msgs))
	}
	if msgs[0].Body != "older-note" || msgs[1].Body != "newer-offer" {
		t.Fatalf("order %#v", msgs)
	}
}

func TestAssignFilterAndReopen(t *testing.T) {
	t.Parallel()
	srv := newServer(t, nil)
	convo := postInbound(t, srv, map[string]any{
		"providerMessageId": "SM_agent",
		"locationId":        "loc_9",
		"channel":           "email",
		"from":              "Customer@Example.com",
		"to":                "inbox@example.com",
		"body":              "Need help",
		"sentAt":            time.Now().UTC().Format(time.RFC3339),
	})
	patch(t, srv, convo.ID, map[string]any{"assignedTo": "agent_7", "status": "closed"})
	closed := listConversations(t, srv, "loc_9", "status=closed&assignedTo=agent_7")
	if len(closed.Data) != 1 || closed.Data[0].Status != "closed" {
		t.Fatalf("closed filter %#v", closed.Data)
	}
	if open := listConversations(t, srv, "loc_9", "status=open"); len(open.Data) != 0 {
		t.Fatalf("open filter %#v", open.Data)
	}
	code, _ := do(t, http.MethodPost, srv.URL+"/conversations/"+convo.ID+"/read", nil)
	if code != http.StatusOK {
		t.Fatalf("read status %d", code)
	}
	reopened := postInbound(t, srv, map[string]any{
		"providerMessageId": "SM_agent_2",
		"locationId":        "loc_9",
		"channel":           "email",
		"from":              "customer@example.com",
		"to":                "inbox@example.com",
		"body":              "following up",
		"sentAt":            time.Now().UTC().Add(time.Minute).Format(time.RFC3339),
	})
	if reopened.ID != convo.ID || reopened.Status != "open" || reopened.Unread != 1 {
		t.Fatalf("reopen %#v", reopened)
	}
	code, body := do(t, http.MethodPost, srv.URL+"/conversations/"+convo.ID+"/messages", map[string]any{
		"channel": "sms",
		"body":    "nope",
	})
	if code != http.StatusBadRequest {
		t.Fatalf("email contact accepted sms: %d %s", code, body)
	}
}

func TestSearchAndTyping(t *testing.T) {
	t.Parallel()
	srv := newServer(t, nil)
	convo := postInbound(t, srv, inbound("+15550000010", "SM_search", "sms", "Is the offer still valid?", time.Now().UTC()))
	stream := openSSE(t, srv.URL+"/locations/loc_1/events")
	t.Cleanup(func() { stream.body.Close() })
	waitConnected(t, stream.r)

	code, _ := do(t, http.MethodPost, srv.URL+"/conversations/"+convo.ID+"/typing", map[string]any{
		"agentId": "agent_7",
		"typing":  true,
	})
	if code != http.StatusNoContent {
		t.Fatalf("typing status %d", code)
	}
	name, raw, err := readEvent(stream.r, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if name != "typing" || !strings.Contains(raw, "agent_7") {
		t.Fatalf("event %s %s", name, raw)
	}

	page := listSearch(t, srv, "loc_1", "offer")
	if len(page.Data) != 1 || page.Data[0].ID != convo.LastMessageID {
		t.Fatalf("search %#v", page.Data)
	}
	empty := listSearch(t, srv, "loc_1", "nomatch")
	if len(empty.Data) != 0 {
		t.Fatalf("expected no hits, got %d", len(empty.Data))
	}
}

func TestSSELocationIsolation(t *testing.T) {
	t.Parallel()
	srv := newServer(t, nil)
	mine := openSSE(t, srv.URL+"/locations/loc_1/events")
	other := openSSE(t, srv.URL+"/locations/loc_2/events")
	t.Cleanup(func() {
		mine.body.Close()
		other.body.Close()
	})
	waitConnected(t, mine.r)
	waitConnected(t, other.r)

	postInbound(t, srv, inbound("+15550000011", "SM_rt", "sms", "live", time.Now().UTC()))

	deadline := time.Now().Add(2 * time.Second)
	var saw bool
	for time.Now().Before(deadline) {
		name, raw, err := readEvent(mine.r, time.Until(deadline))
		if err != nil {
			t.Fatal(err)
		}
		if name == "message.created" && strings.Contains(raw, "live") {
			saw = true
			break
		}
	}
	if !saw {
		t.Fatal("location loc_1 did not receive message.created")
	}
	if _, _, err := readEvent(other.r, 200*time.Millisecond); !errors.Is(err, errTimeout) {
		t.Fatalf("location loc_2 received an event: %v", err)
	}
}

func TestHealthAndValidation(t *testing.T) {
	t.Parallel()
	srv := newServer(t, nil)
	code, body := do(t, http.MethodGet, srv.URL+"/healthz", nil)
	if code != http.StatusOK || !strings.Contains(string(body), "ok") {
		t.Fatalf("health %d %s", code, body)
	}
	code, _ = do(t, http.MethodPost, srv.URL+"/webhooks/inbound", map[string]any{"channel": "pigeon"})
	if code != http.StatusBadRequest {
		t.Fatalf("status %d", code)
	}
	code, _ = do(t, http.MethodPost, srv.URL+"/conversations/missing/messages", map[string]any{"channel": "sms", "body": "hi"})
	if code != http.StatusNotFound {
		t.Fatalf("status %d", code)
	}
}

type inboundResult struct {
	Duplicate    bool               `json:"duplicate"`
	Message      model.Message      `json:"message"`
	Conversation model.Conversation `json:"conversation"`
}

type sendResult struct {
	Message      model.Message      `json:"message"`
	Conversation model.Conversation `json:"conversation"`
}

type conversationPage struct {
	Data       []model.Conversation `json:"data"`
	NextCursor *string              `json:"nextCursor"`
}

type messagePage struct {
	Data       []model.Message `json:"data"`
	NextCursor *string         `json:"nextCursor"`
}

type convoSnap struct {
	ID            string
	Status        string
	Unread        int
	Preview       string
	LastMessageID string
}

func newServer(t *testing.T, sender provider.Sender) *httptest.Server {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "inbox.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	if sender == nil {
		sender = provider.NewMock(0)
	}
	hub := realtime.New()
	svc := inbox.New(st, sender, hub, inbox.Config{RetryBase: time.Millisecond, SyncSend: true})
	srv := httptest.NewServer(httpapi.New(svc, hub))
	t.Cleanup(srv.Close)
	return srv
}

func inbound(from, providerID, channel, body string, sent time.Time) map[string]any {
	to := "+918000000000"
	if channel == "email" {
		to = "inbox@example.com"
	}
	return map[string]any{
		"providerMessageId": providerID,
		"locationId":        "loc_1",
		"channel":           channel,
		"from":              from,
		"to":                to,
		"body":              body,
		"sentAt":            sent.Format(time.RFC3339),
	}
}

func postInbound(t *testing.T, srv *httptest.Server, payload any) convoSnap {
	t.Helper()
	code, body := do(t, http.MethodPost, srv.URL+"/webhooks/inbound", payload)
	if code != http.StatusCreated && code != http.StatusOK {
		t.Fatalf("inbound %d %s", code, body)
	}
	res := decode[inboundResult](t, body)
	return convoSnap{
		ID:            res.Conversation.ID,
		Status:        res.Conversation.Status,
		Unread:        res.Conversation.UnreadCount,
		Preview:       res.Conversation.LastMessagePreview,
		LastMessageID: res.Message.ID,
	}
}

func postSend(t *testing.T, srv *httptest.Server, conversationID, channel, body string) sendResult {
	t.Helper()
	code, raw := do(t, http.MethodPost, srv.URL+"/conversations/"+conversationID+"/messages", map[string]any{
		"channel": channel,
		"body":    body,
	})
	if code != http.StatusAccepted {
		t.Fatalf("send %d %s", code, raw)
	}
	return decode[sendResult](t, raw)
}

func postStatus(t *testing.T, srv *httptest.Server, providerID, st string) {
	t.Helper()
	code, body := do(t, http.MethodPost, srv.URL+"/webhooks/status", map[string]any{
		"providerMessageId": providerID,
		"status":            st,
		"timestamp":         time.Now().UTC().Format(time.RFC3339),
	})
	if code != http.StatusOK {
		t.Fatalf("status %d %s", code, body)
	}
}

func patch(t *testing.T, srv *httptest.Server, id string, payload any) {
	t.Helper()
	code, body := do(t, http.MethodPatch, srv.URL+"/conversations/"+id, payload)
	if code != http.StatusOK {
		t.Fatalf("patch %d %s", code, body)
	}
}

func listConversations(t *testing.T, srv *httptest.Server, locationID, query string) conversationPage {
	t.Helper()
	url := srv.URL + "/locations/" + locationID + "/conversations"
	if query != "" {
		url += "?" + query
	}
	code, body := do(t, http.MethodGet, url, nil)
	if code != http.StatusOK {
		t.Fatalf("list %d %s", code, body)
	}
	return decode[conversationPage](t, body)
}

func getMessages(t *testing.T, srv *httptest.Server, conversationID, cursor string, limit int) messagePage {
	t.Helper()
	url := fmt.Sprintf("%s/conversations/%s/messages?limit=%d", srv.URL, conversationID, limit)
	if cursor != "" {
		url += "&cursor=" + cursor
	}
	code, body := do(t, http.MethodGet, url, nil)
	if code != http.StatusOK {
		t.Fatalf("messages %d %s", code, body)
	}
	return decode[messagePage](t, body)
}

func listSearch(t *testing.T, srv *httptest.Server, locationID, q string) messagePage {
	t.Helper()
	code, body := do(t, http.MethodGet, srv.URL+"/locations/"+locationID+"/messages/search?q="+q, nil)
	if code != http.StatusOK {
		t.Fatalf("search %d %s", code, body)
	}
	return decode[messagePage](t, body)
}

func allMessages(t *testing.T, srv *httptest.Server, conversationID string) []model.Message {
	t.Helper()
	var out []model.Message
	cursor := ""
	for {
		page := getMessages(t, srv, conversationID, cursor, 100)
		out = append(out, page.Data...)
		if page.NextCursor == nil {
			return out
		}
		cursor = *page.NextCursor
	}
}

func mustMessage(t *testing.T, srv *httptest.Server, conversationID, messageID string) model.Message {
	t.Helper()
	for _, m := range allMessages(t, srv, conversationID) {
		if m.ID == messageID {
			return m
		}
	}
	t.Fatalf("message %s not found", messageID)
	return model.Message{}
}

func do(t *testing.T, method, url string, payload any) (int, []byte) {
	t.Helper()
	code, body, err := doRaw(method, url, payload)
	if err != nil {
		t.Fatal(err)
	}
	return code, body
}

func doRaw(method, url string, payload any) (int, []byte, error) {
	var r io.Reader
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return 0, nil, err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, r)
	if err != nil {
		return 0, nil, err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	return res.StatusCode, body, err
}

func decode[T any](t *testing.T, body []byte) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(body, &v); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	return v
}

type scripted struct {
	failFirst int
	before    func(providerID string)
	mu        sync.Mutex
	calls     int
	id        string
}

func (s *scripted) Send(_ context.Context, msg provider.Outbound) (string, error) {
	s.mu.Lock()
	s.calls++
	call := s.calls
	if s.id == "" {
		s.id = "prov_" + msg.MessageID
	}
	id := s.id
	failFirst := s.failFirst
	before := s.before
	s.mu.Unlock()
	if before != nil && call == 1 {
		before(id)
	}
	if call <= failFirst {
		return "", errors.New("mock provider rejected the send")
	}
	return id, nil
}

type sseStream struct {
	body io.ReadCloser
	r    *bufio.Reader
}

func openSSE(t *testing.T, url string) *sseStream {
	t.Helper()
	res, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	if res.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(res.Body)
		res.Body.Close()
		t.Fatalf("sse %d %s", res.StatusCode, b)
	}
	return &sseStream{body: res.Body, r: bufio.NewReader(res.Body)}
}

func waitConnected(t *testing.T, r *bufio.Reader) {
	t.Helper()
	line, err := r.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(line, ":") {
		t.Fatalf("expected SSE comment, got %q", line)
	}
}

var errTimeout = errors.New("timeout")

func readEvent(r *bufio.Reader, timeout time.Duration) (string, string, error) {
	type result struct {
		name string
		raw  string
		err  error
	}
	ch := make(chan result, 1)
	go func() {
		var name, data string
		for {
			line, err := r.ReadString('\n')
			if err != nil {
				ch <- result{err: err}
				return
			}
			line = strings.TrimRight(line, "\r\n")
			if line == "" {
				if name != "" || data != "" {
					ch <- result{name: name, raw: data}
					return
				}
				continue
			}
			if strings.HasPrefix(line, ":") {
				continue
			}
			if rest, ok := strings.CutPrefix(line, "event:"); ok {
				name = strings.TrimSpace(rest)
			}
			if rest, ok := strings.CutPrefix(line, "data:"); ok {
				data = strings.TrimSpace(rest)
			}
		}
	}()
	select {
	case res := <-ch:
		return res.name, res.raw, res.err
	case <-time.After(timeout):
		return "", "", errTimeout
	}
}
