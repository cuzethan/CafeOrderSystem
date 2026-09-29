package ws

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/cuzethan/CafeOrderSystem/internal/service"
	"github.com/gorilla/websocket"
)

// A real WebSocket client should receive the connect snapshot, then broadcasts,
// and drop off the hub when the socket closes.
func TestHandleKitchenWSUpgradeSnapshotAndUnregister(t *testing.T) {
	hub := NewHub()
	hub.SetSnapshotProvider(func() []service.Order {
		return []service.Order{{ID: "ord-1", Status: "accepted"}}
	})

	mux := http.NewServeMux()
	mux.HandleFunc("GET /ws/kitchen", hub.HandleKitchenWS)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http") + "/ws/kitchen"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var snap Event
	if err := conn.ReadJSON(&snap); err != nil {
		t.Fatalf("read snapshot: %v", err)
	}
	if snap.Type != "orders.snapshot" || len(snap.Orders) != 1 || snap.Orders[0].ID != "ord-1" {
		t.Fatalf("snapshot = %#v", snap)
	}

	hub.Broadcast(Event{Type: "order.updated", OrderID: "ord-1", Status: "preparing"})
	var upd Event
	if err := conn.ReadJSON(&upd); err != nil {
		t.Fatalf("read update: %v", err)
	}
	if upd.Type != "order.updated" || upd.OrderID != "ord-1" || upd.Status != "preparing" {
		t.Fatalf("update = %#v", upd)
	}

	_ = conn.Close()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		hub.mu.RLock()
		n := len(hub.clients)
		hub.mu.RUnlock()
		if n == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("client still registered after close")
}
