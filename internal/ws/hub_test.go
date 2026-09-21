package ws

import (
	"sync"
	"testing"

	"github.com/cuzethan/CafeOrderSystem/internal/service"
)

// Kitchen hub tests use fake clients (in-memory message sinks) instead of real
// WebSocket connections. That lets us verify broadcast/snapshot rules without
// opening network sockets.

// When an event is broadcast, every registered client should receive it.
func TestHubBroadcastsToAllClients(t *testing.T) {
	hub := NewHub()
	c1 := &fakeClient{id: "c1"}
	c2 := &fakeClient{id: "c2"}
	hub.Register(c1)
	hub.Register(c2)

	event := Event{Type: "order.updated", OrderID: "ord-1", Status: "accepted"}
	hub.Broadcast(event)

	assertEvent(t, c1, event)
	assertEvent(t, c2, event)
}

// Unregistered clients must not receive further broadcasts.
func TestHubDoesNotBroadcastToUnregisteredClients(t *testing.T) {
	hub := NewHub()
	c1 := &fakeClient{id: "c1"}
	hub.Register(c1)
	hub.Unregister(c1)

	hub.Broadcast(Event{Type: "order.updated", OrderID: "ord-1", Status: "ready"})

	if len(c1.messages) != 0 {
		t.Fatalf("expected no messages after unregister, got %#v", c1.messages)
	}
}

// On register, the hub should push a snapshot of currently open orders so a
// freshly opened kitchen display is not blank.
func TestHubSendsSnapshotOnRegister(t *testing.T) {
	open := []service.Order{
		{ID: "ord-1", Status: "accepted"},
		{ID: "ord-2", Status: "preparing"},
	}
	hub := NewHub()
	hub.SetSnapshotProvider(func() []service.Order {
		return open
	})

	c := &fakeClient{id: "kitchen-1"}
	hub.Register(c)

	if len(c.messages) != 1 {
		t.Fatalf("expected 1 snapshot message, got %d (%#v)", len(c.messages), c.messages)
	}
	got := c.messages[0]
	if got.Type != "orders.snapshot" {
		t.Fatalf("type = %q, want orders.snapshot", got.Type)
	}
	if len(got.Orders) != 2 {
		t.Fatalf("snapshot orders = %d, want 2", len(got.Orders))
	}
}

func assertEvent(t *testing.T, c *fakeClient, want Event) {
	t.Helper()
	if len(c.messages) != 1 {
		t.Fatalf("client %s got %d messages, want 1 (%#v)", c.id, len(c.messages), c.messages)
	}
	got := c.messages[0]
	if got.Type != want.Type || got.OrderID != want.OrderID || got.Status != want.Status {
		t.Fatalf("client %s got %#v, want %#v", c.id, got, want)
	}
}

// fakeClient records messages the hub tries to send.
type fakeClient struct {
	id       string
	mu       sync.Mutex
	messages []Event
}

func (c *fakeClient) Send(event Event) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.messages = append(c.messages, event)
	return nil
}
