package provider

import (
	"context"
	"errors"
	"math/rand"
	"sync"
	"time"

	"unifiedconvo/internal/id"
)

// Mock fails FailRate of the time (0.2 by default) and otherwise
// returns a synthetic provider message id.
type Mock struct {
	FailRate float64
	mu       sync.Mutex
	rng      *rand.Rand
}

// NewMock returns a provider with a private random source.
func NewMock(failRate float64) *Mock {
	return &Mock{
		FailRate: failRate,
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Send implements Sender.
func (m *Mock) Send(_ context.Context, _ Outbound) (string, error) {
	m.mu.Lock()
	fail := m.rng.Float64() < m.FailRate
	m.mu.Unlock()
	if fail {
		return "", errors.New("mock provider rejected the send")
	}
	return "mock_" + id.New(), nil
}
