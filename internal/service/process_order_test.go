package service

import (
	"errors"
	"testing"

	"github.com/cuzethan/CafeOrderSystem/internal/domain"
)

// These tests cover ProcessOrder — the logic the RabbitMQ worker will call
// when it pulls an order_id off the queue.
//
// Remember: RabbitMQ only delivers the message; ProcessOrder is OUR code that
// updates the order and notifies the kitchen.

// Normal worker path: pending order becomes accepted, kitchen gets notified once.
func TestProcessOrderAcceptsPending(t *testing.T) {
	// Pre-load the fake DB with an order already in pending (as if Create ran).
	store := &fakeOrderStore{
		byKey: map[string]Order{},
		byID: map[string]Order{
			"ord-1": {ID: "ord-1", Status: domain.StatusPending},
		},
	}
	notifier := &fakeNotifier{} // stand-in for the WebSocket hub
	svc := NewOrderService(store, &fakeMenuStore{items: map[string]domain.MenuItem{}}, &fakePublisher{}, notifier)

	if err := svc.ProcessOrder("ord-1"); err != nil {
		t.Fatalf("ProcessOrder: %v", err)
	}
	if store.byID["ord-1"].Status != domain.StatusAccepted {
		t.Fatalf("status = %s, want accepted", store.byID["ord-1"].Status)
	}
	if store.updateCalls != 1 {
		t.Fatalf("updateCalls = %d, want 1", store.updateCalls)
	}
	if notifier.calls != 1 || notifier.lastID != "ord-1" {
		t.Fatalf("notifier calls=%d id=%q", notifier.calls, notifier.lastID)
	}
}

// If RabbitMQ redelivers the same message after we already accepted the order,
// ProcessOrder must succeed without updating or notifying again (idempotent).
func TestProcessOrderIdempotentWhenAlreadyAccepted(t *testing.T) {
	store := &fakeOrderStore{
		byKey: map[string]Order{},
		byID: map[string]Order{
			"ord-1": {ID: "ord-1", Status: domain.StatusAccepted},
		},
	}
	notifier := &fakeNotifier{}
	svc := NewOrderService(store, &fakeMenuStore{items: map[string]domain.MenuItem{}}, &fakePublisher{}, notifier)

	if err := svc.ProcessOrder("ord-1"); err != nil {
		t.Fatalf("ProcessOrder: %v", err)
	}
	if store.updateCalls != 0 {
		t.Fatalf("updateCalls = %d, want 0", store.updateCalls)
	}
	if notifier.calls != 0 {
		t.Fatalf("notifier calls = %d, want 0", notifier.calls)
	}
}

// Unknown order id should return our ErrOrderNotFound sentinel.
func TestProcessOrderRejectsMissingOrder(t *testing.T) {
	store := &fakeOrderStore{byKey: map[string]Order{}, byID: map[string]Order{}}
	svc := NewOrderService(store, &fakeMenuStore{items: map[string]domain.MenuItem{}}, &fakePublisher{}, nil)

	err := svc.ProcessOrder("missing")
	if !errors.Is(err, ErrOrderNotFound) {
		t.Fatalf("err = %v, want ErrOrderNotFound", err)
	}
}

// fakeNotifier stands in for "push an update to kitchen WebSocket clients".
type fakeNotifier struct {
	calls  int
	lastID string
}

func (f *fakeNotifier) NotifyOrderUpdated(order Order) error {
	f.calls++
	f.lastID = order.ID
	return nil
}
