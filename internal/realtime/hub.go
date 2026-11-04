// Package realtime is an in-process pub/sub hub keyed by location.
// Each server instance only fans out to agents connected to that process.
package realtime

import "sync"

// Event is a server-pushed notification.
type Event struct {
	Type       string `json:"type"`
	LocationID string `json:"locationId"`
	Data       any    `json:"data"`
}

// Hub delivers events to subscribers of a single location.
type Hub struct {
	mu   sync.Mutex
	subs map[string]map[chan Event]struct{}
}

// New returns an empty hub.
func New() *Hub {
	return &Hub{subs: map[string]map[chan Event]struct{}{}}
}

// Subscribe registers a buffered channel for locationID.
// The returned function removes it and is safe to call more than once.
func (h *Hub) Subscribe(locationID string) (<-chan Event, func()) {
	ch := make(chan Event, 64)
	h.mu.Lock()
	set := h.subs[locationID]
	if set == nil {
		set = map[chan Event]struct{}{}
		h.subs[locationID] = set
	}
	set[ch] = struct{}{}
	h.mu.Unlock()

	var once sync.Once
	unsubscribe := func() {
		once.Do(func() {
			h.mu.Lock()
			delete(h.subs[locationID], ch)
			if len(h.subs[locationID]) == 0 {
				delete(h.subs, locationID)
			}
			h.mu.Unlock()
		})
	}
	return ch, unsubscribe
}

// Publish sends ev to subscribers of ev.LocationID.
// A subscriber with a full buffer is skipped so one slow agent cannot stall the webhook.
func (h *Hub) Publish(ev Event) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for ch := range h.subs[ev.LocationID] {
		select {
		case ch <- ev:
		default:
		}
	}
}
