package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"

	"unifiedconvo/internal/apperr"
	"unifiedconvo/internal/inbox"
	"unifiedconvo/internal/realtime"
)

// New returns the HTTP handler, including the SSE stream.
func New(svc *inbox.Service, hub *realtime.Hub) http.Handler {
	h := &handler{svc: svc, hub: hub}
	r := chi.NewRouter()
	r.Use(middleware.Recoverer)
	r.Get("/healthz", h.health)
	r.Post("/webhooks/inbound", h.inbound)
	r.Post("/webhooks/status", h.status)
	r.Get("/locations/{locationId}/conversations", h.listConversations)
	r.Get("/locations/{locationId}/messages/search", h.search)
	r.Get("/locations/{locationId}/events", h.events)
	r.Post("/conversations/{id}/messages", h.send)
	r.Get("/conversations/{id}/messages", h.listMessages)
	r.Post("/conversations/{id}/read", h.markRead)
	r.Patch("/conversations/{id}", h.patch)
	r.Post("/conversations/{id}/typing", h.typing)
	return r
}

type handler struct {
	svc *inbox.Service
	hub *realtime.Hub
}

func (h *handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

type inboundBody struct {
	ProviderMessageID string    `json:"providerMessageId"`
	LocationID        string    `json:"locationId"`
	Channel           string    `json:"channel"`
	From              string    `json:"from"`
	To                string    `json:"to"`
	Body              string    `json:"body"`
	SentAt            time.Time `json:"sentAt"`
}

func (h *handler) inbound(w http.ResponseWriter, r *http.Request) {
	var body inboundBody
	if err := decode(w, r, &body); err != nil {
		writeError(w, err)
		return
	}
	result, err := h.svc.Inbound(r.Context(), inbox.InboundInput{
		ProviderMessageID: body.ProviderMessageID,
		LocationID:        body.LocationID,
		Channel:           body.Channel,
		From:              body.From,
		To:                body.To,
		Body:              body.Body,
		SentAt:            body.SentAt,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	code := http.StatusCreated
	if result.Duplicate {
		code = http.StatusOK
	}
	writeJSON(w, code, result)
}

type statusBody struct {
	ProviderMessageID string    `json:"providerMessageId"`
	Status            string    `json:"status"`
	Timestamp         time.Time `json:"timestamp"`
}

func (h *handler) status(w http.ResponseWriter, r *http.Request) {
	var body statusBody
	if err := decode(w, r, &body); err != nil {
		writeError(w, err)
		return
	}
	result, err := h.svc.Status(r.Context(), inbox.StatusInput{
		ProviderMessageID: body.ProviderMessageID,
		Status:            body.Status,
		Timestamp:         body.Timestamp,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

type sendBody struct {
	Channel string `json:"channel"`
	Body    string `json:"body"`
}

func (h *handler) send(w http.ResponseWriter, r *http.Request) {
	var body sendBody
	if err := decode(w, r, &body); err != nil {
		writeError(w, err)
		return
	}
	result, err := h.svc.Send(r.Context(), chi.URLParam(r, "id"), inbox.OutboundInput{
		Channel: body.Channel,
		Body:    body.Body,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, result)
}

func (h *handler) listConversations(w http.ResponseWriter, r *http.Request) {
	limit, err := queryLimit(r)
	if err != nil {
		writeError(w, err)
		return
	}
	q := r.URL.Query()
	page, err := h.svc.ListConversations(r.Context(), chi.URLParam(r, "locationId"), inbox.ConversationFilter{
		Status:     q.Get("status"),
		AssignedTo: q.Get("assignedTo"),
		Cursor:     q.Get("cursor"),
		Limit:      limit,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writePage(w, page.Data, page.NextCursor)
}

func (h *handler) listMessages(w http.ResponseWriter, r *http.Request) {
	limit, err := queryLimit(r)
	if err != nil {
		writeError(w, err)
		return
	}
	page, err := h.svc.ListMessages(r.Context(), chi.URLParam(r, "id"), r.URL.Query().Get("cursor"), limit)
	if err != nil {
		writeError(w, err)
		return
	}
	writePage(w, page.Data, page.NextCursor)
}

func (h *handler) markRead(w http.ResponseWriter, r *http.Request) {
	convo, err := h.svc.MarkRead(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, convo)
}

type patchBody struct {
	AssignedTo *string `json:"assignedTo"`
	Status     *string `json:"status"`
}

func (h *handler) patch(w http.ResponseWriter, r *http.Request) {
	var body patchBody
	if err := decode(w, r, &body); err != nil {
		writeError(w, err)
		return
	}
	convo, err := h.svc.Patch(r.Context(), chi.URLParam(r, "id"), inbox.PatchInput{
		AssignedTo: body.AssignedTo,
		Status:     body.Status,
	})
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, convo)
}

func (h *handler) search(w http.ResponseWriter, r *http.Request) {
	limit, err := queryLimit(r)
	if err != nil {
		writeError(w, err)
		return
	}
	page, err := h.svc.Search(r.Context(), chi.URLParam(r, "locationId"), r.URL.Query().Get("q"), r.URL.Query().Get("cursor"), limit)
	if err != nil {
		writeError(w, err)
		return
	}
	writePage(w, page.Data, page.NextCursor)
}

type typingBody struct {
	AgentID string `json:"agentId"`
	Typing  *bool  `json:"typing"`
}

func (h *handler) typing(w http.ResponseWriter, r *http.Request) {
	var body typingBody
	if err := decode(w, r, &body); err != nil {
		writeError(w, err)
		return
	}
	if body.Typing == nil {
		writeError(w, apperr.Invalid("typing is required"))
		return
	}
	if err := h.svc.Typing(r.Context(), chi.URLParam(r, "id"), body.AgentID, *body.Typing); err != nil {
		writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *handler) events(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		writeError(w, fmt.Errorf("streaming is not supported"))
		return
	}
	locationID := chi.URLParam(r, "locationId")
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	events, unsubscribe := h.hub.Subscribe(locationID)
	defer unsubscribe()

	fmt.Fprintf(w, ": connected\n\n")
	flusher.Flush()

	for {
		select {
		case <-r.Context().Done():
			return
		case ev := <-events:
			payload, err := json.Marshal(ev)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Type, payload)
			flusher.Flush()
		}
	}
}

func decode(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(dst); err != nil {
		return apperr.Invalid("malformed json")
	}
	return nil
}

func queryLimit(r *http.Request) (int, error) {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, apperr.Invalid("limit must be between 1 and 100")
	}
	return n, nil
}

func writePage(w http.ResponseWriter, data any, next string) {
	var cursor *string
	if next != "" {
		cursor = &next
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"data":       data,
		"nextCursor": cursor,
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, apperr.ErrNotFound):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
	case errors.Is(err, apperr.ErrInvalid):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
	default:
		if errors.Is(err, io.EOF) {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "malformed json"})
			return
		}
		log.Printf("request error: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
	}
}
