package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cuzethan/CafeOrderSystem/internal/domain"
	"github.com/cuzethan/CafeOrderSystem/internal/service"
	"github.com/cuzethan/CafeOrderSystem/internal/ws"
	"github.com/gorilla/websocket"
)

// HTTP handler tests use httptest (fake request/response) and the same in-memory
// fakes as the service tests — still no real Postgres or network port.

func TestHealth(t *testing.T) {
	h := newTestHandler()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rec := httptest.NewRecorder()
	h.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

func TestGetMenuReturnsAvailableItems(t *testing.T) {
	h := newTestHandler()
	h.menu.items["latte"] = domain.MenuItem{ID: "latte", Name: "Latte", PriceCents: 450, Available: true}
	h.menu.items["soup"] = domain.MenuItem{ID: "soup", Name: "Soup", PriceCents: 600, Available: false}

	req := httptest.NewRequest(http.MethodGet, "/menu", nil)
	rec := httptest.NewRecorder()
	h.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body struct {
		Items []domain.MenuItem `json:"items"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(body.Items) != 1 || body.Items[0].ID != "latte" {
		t.Fatalf("items = %#v, want only latte", body.Items)
	}
}

func TestCreateOrderAccepted(t *testing.T) {
	h := newTestHandler()
	h.menu.items["latte"] = domain.MenuItem{ID: "latte", Name: "Latte", PriceCents: 450, Available: true}

	payload, _ := json.Marshal(map[string]any{
		"kiosk_id": "kiosk-1",
		"items":    []map[string]any{{"menu_item_id": "latte", "quantity": 1}},
	})
	req := httptest.NewRequest(http.MethodPost, "/orders", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "key-abc")
	rec := httptest.NewRecorder()
	h.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusAccepted {
		t.Fatalf("status = %d body=%s, want 202", rec.Code, rec.Body.String())
	}
}

func TestCreateOrderRequiresIdempotencyKey(t *testing.T) {
	h := newTestHandler()
	h.menu.items["latte"] = domain.MenuItem{ID: "latte", Name: "Latte", PriceCents: 450, Available: true}

	payload, _ := json.Marshal(map[string]any{
		"kiosk_id": "kiosk-1",
		"items":    []map[string]any{{"menu_item_id": "latte", "quantity": 1}},
	})
	req := httptest.NewRequest(http.MethodPost, "/orders", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestKitchenSocketUnavailableWithoutHub(t *testing.T) {
	h := newTestHandler()
	req := httptest.NewRequest(http.MethodGet, "/ws/kitchen", nil)
	rec := httptest.NewRecorder()
	h.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}
}

func TestKitchenSocketUpgradeThroughRoutes(t *testing.T) {
	h := newTestHandler()
	hub := ws.NewHub()
	hub.SetSnapshotProvider(func() []service.Order {
		return []service.Order{{ID: "ord-1", Status: "accepted"}}
	})
	h.Hub = hub

	srv := httptest.NewServer(h.Routes())
	t.Cleanup(srv.Close)

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http") + "/ws/kitchen"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var snap ws.Event
	if err := conn.ReadJSON(&snap); err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	if snap.Type != "orders.snapshot" || len(snap.Orders) != 1 || snap.Orders[0].ID != "ord-1" {
		t.Fatalf("snapshot = %#v", snap)
	}

	hub.Broadcast(ws.Event{Type: "order.updated", OrderID: "ord-1", Status: "preparing"})
	var upd ws.Event
	if err := conn.ReadJSON(&upd); err != nil {
		t.Fatalf("read update: %v", err)
	}
	if upd.Type != "order.updated" || upd.OrderID != "ord-1" || upd.Status != "preparing" {
		t.Fatalf("update = %#v", upd)
	}
}

func TestUpdateStatusConflictOnIllegalTransition(t *testing.T) {
	h := newTestHandler()
	h.menu.items["latte"] = domain.MenuItem{ID: "latte", Name: "Latte", PriceCents: 450, Available: true}

	order, err := h.Orders.Create(service.CreateOrderInput{
		KioskID:        "kiosk-1",
		IdempotencyKey: "key-1",
		Items:          []domain.LineItemInput{{MenuItemID: "latte", Quantity: 1}},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	payload, _ := json.Marshal(map[string]string{"status": "ready"}) // illegal: pending -> ready
	req := httptest.NewRequest(http.MethodPatch, "/orders/"+order.ID+"/status", bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	h.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d body=%s, want 409", rec.Code, rec.Body.String())
	}
}

type testHandler struct {
	*Handler
	menu  *fakeMenu
	store *fakeStore
	pub   *fakePub
}

func newTestHandler() *testHandler {
	store := &fakeStore{byKey: map[string]service.Order{}, byID: map[string]service.Order{}}
	menu := &fakeMenu{items: map[string]domain.MenuItem{}}
	pub := &fakePub{}
	orders := service.NewOrderService(store, menu, pub, nil)
	h := &Handler{Orders: orders, Menu: menu}
	return &testHandler{Handler: h, menu: menu, store: store, pub: pub}
}

type fakeStore struct {
	byKey map[string]service.Order
	byID  map[string]service.Order
}

func (f *fakeStore) GetByIdempotencyKey(key string) (service.Order, bool, error) {
	o, ok := f.byKey[key]
	return o, ok, nil
}
func (f *fakeStore) GetByID(id string) (service.Order, bool, error) {
	o, ok := f.byID[id]
	return o, ok, nil
}
func (f *fakeStore) Create(order service.Order) error {
	f.byKey[order.IdempotencyKey] = order
	f.byID[order.ID] = order
	return nil
}
func (f *fakeStore) UpdateStatus(id string, status domain.Status) error {
	o := f.byID[id]
	o.Status = status
	f.byID[id] = o
	if o.IdempotencyKey != "" {
		f.byKey[o.IdempotencyKey] = o
	}
	return nil
}

type fakeMenu struct {
	items map[string]domain.MenuItem
}

func (f *fakeMenu) GetMenu() (map[string]domain.MenuItem, error) { return f.items, nil }

type fakePub struct{}

func (fakePub) PublishOrderCreated(string) error { return nil }
