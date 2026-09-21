package httpapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cuzethan/CafeOrderSystem/internal/domain"
	"github.com/cuzethan/CafeOrderSystem/internal/service"
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
