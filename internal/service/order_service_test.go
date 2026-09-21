package service

import (
	"errors"
	"testing"

	"github.com/cuzethan/CafeOrderSystem/internal/domain"
)

// These tests exercise OrderService.Create with in-memory fakes instead of
// real Postgres / RabbitMQ. That keeps tests fast and focused on business rules.
//
// Pattern in every test:
//  1. Build a service with fakes (newTestOrderService).
//  2. Call Create(...).
//  3. Assert the result AND that side effects happened the right number of times
//     (store.createCalls, pub.publishCalls).

// Bad cart should error and must not write to the DB or queue.
func TestCreateOrderRejectsEmptyCart(t *testing.T) {
	svc := newTestOrderService()

	_, err := svc.Create(CreateOrderInput{
		KioskID:        "kiosk-1",
		IdempotencyKey: "key-1",
		Items:          nil, // empty cart
	})
	if err == nil {
		t.Fatal("expected error for empty cart")
	}
	// Side-effect checks: validation failed early, so nothing was saved/queued.
	if svc.store.createCalls != 0 {
		t.Fatalf("expected no store create, got %d", svc.store.createCalls)
	}
	if svc.pub.publishCalls != 0 {
		t.Fatalf("expected no publish, got %d", svc.pub.publishCalls)
	}
}

// Happy path: valid cart → pending order, correct total, one DB write, one publish.
func TestCreateOrderPersistsPendingAndPublishes(t *testing.T) {
	svc := newTestOrderService()
	// Seed the fake menu so "latte" looks available (like a seeded Postgres row).
	svc.menu.items["latte"] = domain.MenuItem{
		ID: "latte", Name: "Latte", PriceCents: 450, Available: true,
	}

	order, err := svc.Create(CreateOrderInput{
		KioskID:        "kiosk-1",
		IdempotencyKey: "key-1",
		CustomerName:   "Alex",
		Items: []domain.LineItemInput{
			{MenuItemID: "latte", Quantity: 2}, // 2 * 450 = 900 cents
		},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if order.Status != domain.StatusPending {
		t.Fatalf("status = %s, want pending", order.Status)
	}
	if order.TotalCents != 900 {
		t.Fatalf("total = %d, want 900", order.TotalCents)
	}
	if len(order.Items) != 1 || order.Items[0].Quantity != 2 {
		t.Fatalf("unexpected items: %+v", order.Items)
	}
	if svc.store.createCalls != 1 {
		t.Fatalf("createCalls = %d, want 1", svc.store.createCalls)
	}
	// Publish should use the new order's ID (what the worker will consume later).
	if svc.pub.publishCalls != 1 || svc.pub.lastOrderID != order.ID {
		t.Fatalf("publishCalls=%d last=%q want 1 and %q", svc.pub.publishCalls, svc.pub.lastOrderID, order.ID)
	}
}

// Idempotency: if the kiosk retries with the same Idempotency-Key (network glitch,
// double-tap), we return the SAME order and do not create/publish a second time.
func TestCreateOrderIdempotentReplayReturnsSameOrderWithoutRepublish(t *testing.T) {
	svc := newTestOrderService()
	svc.menu.items["muffin"] = domain.MenuItem{
		ID: "muffin", Name: "Muffin", PriceCents: 300, Available: true,
	}

	// First submit — creates and publishes.
	first, err := svc.Create(CreateOrderInput{
		KioskID:        "kiosk-1",
		IdempotencyKey: "same-key",
		Items:          []domain.LineItemInput{{MenuItemID: "muffin", Quantity: 1}},
	})
	if err != nil {
		t.Fatalf("first Create: %v", err)
	}

	// Second submit — same key; should be a no-op replay.
	second, err := svc.Create(CreateOrderInput{
		KioskID:        "kiosk-1",
		IdempotencyKey: "same-key",
		Items:          []domain.LineItemInput{{MenuItemID: "muffin", Quantity: 1}},
	})
	if err != nil {
		t.Fatalf("second Create: %v", err)
	}
	if first.ID != second.ID {
		t.Fatalf("ids differ: %s vs %s", first.ID, second.ID)
	}
	if svc.store.createCalls != 1 {
		t.Fatalf("createCalls = %d, want 1", svc.store.createCalls)
	}
	if svc.pub.publishCalls != 1 {
		t.Fatalf("publishCalls = %d, want 1", svc.pub.publishCalls)
	}
}

// Every create must include an Idempotency-Key header value.
func TestCreateOrderRequiresIdempotencyKey(t *testing.T) {
	svc := newTestOrderService()
	_, err := svc.Create(CreateOrderInput{
		KioskID: "kiosk-1",
		// IdempotencyKey intentionally left empty
		Items: []domain.LineItemInput{{MenuItemID: "latte", Quantity: 1}},
	})
	// errors.Is checks "is this the specific sentinel error we defined?"
	if !errors.Is(err, ErrIdempotencyKeyRequired) {
		t.Fatalf("err = %v, want ErrIdempotencyKeyRequired", err)
	}
}

// --- Test helpers (fakes) below ---
//
// In Go, interfaces are satisfied implicitly: if a type has the right methods,
// it implements the interface. fakeOrderStore implements OrderStore, etc.
// We can pass fakes into NewOrderService without a real database.

// orderServiceHarness bundles the real service with pointers to its fakes
// so tests can inspect counters like createCalls.
type orderServiceHarness struct {
	*OrderService // embedded: harness can call svc.Create(...) directly
	store         *fakeOrderStore
	menu          *fakeMenuStore
	pub           *fakePublisher
}

func newTestOrderService() *orderServiceHarness {
	store := &fakeOrderStore{byKey: map[string]Order{}, byID: map[string]Order{}}
	menu := &fakeMenuStore{items: map[string]domain.MenuItem{}}
	pub := &fakePublisher{}
	return &orderServiceHarness{
		// nil notifier → service uses a no-op (Create does not need kitchen push yet)
		OrderService: NewOrderService(store, menu, pub, nil),
		store:        store,
		menu:         menu,
		pub:          pub,
	}
}

// fakeOrderStore is an in-memory stand-in for Postgres.
type fakeOrderStore struct {
	byKey       map[string]Order // lookup by Idempotency-Key
	byID        map[string]Order // lookup by order ID
	createCalls int              // how many times Create was called
	updateCalls int              // how many times UpdateStatus was called
}

func (f *fakeOrderStore) GetByIdempotencyKey(key string) (Order, bool, error) {
	o, ok := f.byKey[key]
	return o, ok, nil // ok=false means "not found"
}

func (f *fakeOrderStore) GetByID(id string) (Order, bool, error) {
	o, ok := f.byID[id]
	return o, ok, nil
}

func (f *fakeOrderStore) Create(order Order) error {
	f.createCalls++
	f.byKey[order.IdempotencyKey] = order
	f.byID[order.ID] = order
	return nil
}

func (f *fakeOrderStore) UpdateStatus(id string, status domain.Status) error {
	f.updateCalls++
	o := f.byID[id]
	o.Status = status
	f.byID[id] = o
	if o.IdempotencyKey != "" {
		f.byKey[o.IdempotencyKey] = o // keep both indexes in sync
	}
	return nil
}

// fakeMenuStore is an in-memory menu catalog.
type fakeMenuStore struct {
	items map[string]domain.MenuItem
}

func (f *fakeMenuStore) GetMenu() (map[string]domain.MenuItem, error) {
	return f.items, nil
}

// fakePublisher stands in for "put a message on RabbitMQ".
type fakePublisher struct {
	publishCalls int
	lastOrderID  string
}

func (f *fakePublisher) PublishOrderCreated(orderID string) error {
	f.publishCalls++
	f.lastOrderID = orderID
	return nil
}
