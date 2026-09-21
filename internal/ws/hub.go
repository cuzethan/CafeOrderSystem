package ws

import (
	"sync"

	"github.com/cuzethan/CafeOrderSystem/internal/domain"
	"github.com/cuzethan/CafeOrderSystem/internal/service"
)

// Event is a JSON-friendly kitchen update.
type Event struct {
	Type    string          `json:"type"`
	OrderID string          `json:"order_id,omitempty"`
	Status  domain.Status   `json:"status,omitempty"`
	Orders  []service.Order `json:"orders,omitempty"`
}

// Client is anything that can receive kitchen events (real WS or test fake).
type Client interface {
	Send(event Event) error
}

// SnapshotProvider returns currently open orders for new kitchen connections.
type SnapshotProvider func() []service.Order

// Hub fans out events to all registered kitchen clients.
type Hub struct {
	mu       sync.RWMutex
	clients  map[Client]struct{}
	snapshot SnapshotProvider
}

// NewHub creates an empty kitchen hub.
func NewHub() *Hub {
	return &Hub{clients: make(map[Client]struct{})}
}

// SetSnapshotProvider configures what Register sends to new clients.
func (h *Hub) SetSnapshotProvider(fn SnapshotProvider) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.snapshot = fn
}

// Register adds a client and immediately sends an orders.snapshot if configured.
func (h *Hub) Register(c Client) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	snapFn := h.snapshot
	h.mu.Unlock()

	if snapFn == nil {
		return
	}
	_ = c.Send(Event{
		Type:   "orders.snapshot",
		Orders: snapFn(),
	})
}

// Unregister removes a client so it no longer receives broadcasts.
func (h *Hub) Unregister(c Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.clients, c)
}

// Broadcast sends an event to every registered client.
func (h *Hub) Broadcast(event Event) {
	h.mu.RLock()
	clients := make([]Client, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.RUnlock()

	for _, c := range clients {
		_ = c.Send(event)
	}
}

// NotifyOrderUpdated adapts the hub to service.OrderNotifier.
func (h *Hub) NotifyOrderUpdated(order service.Order) error {
	h.Broadcast(Event{
		Type:    "order.updated",
		OrderID: order.ID,
		Status:  order.Status,
	})
	return nil
}
