package ws

import (
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait  = 10 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = (pongWait * 9) / 10
	sendBuffer = 32
)

// upgrader turns GET /ws/kitchen into a WebSocket.
// Any origin is allowed: v1 has no auth, and the display may be opened from
// another host on the cafe LAN.
var upgrader = websocket.Upgrader{
	CheckOrigin: func(*http.Request) bool { return true },
}

// HandleKitchenWS upgrades the request and registers the connection on the hub.
// The new client receives an orders.snapshot, then later broadcasts.
func (h *Hub) HandleKitchenWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	c := &socketClient{
		hub:  h,
		conn: conn,
		send: make(chan Event, sendBuffer),
		done: make(chan struct{}),
	}
	h.Register(c)
	go c.writePump()
	go c.readPump()
}

// socketClient is one kitchen display connection.
type socketClient struct {
	hub  *Hub
	conn *websocket.Conn
	send chan Event
	done chan struct{}
	once sync.Once
}

func (c *socketClient) Send(event Event) error {
	select {
	case <-c.done:
		return errors.New("kitchen client closed")
	case c.send <- event:
		return nil
	default:
		c.stop()
		return errors.New("kitchen client send buffer full")
	}
}

func (c *socketClient) stop() {
	c.once.Do(func() {
		close(c.done)
		c.hub.Unregister(c)
		_ = c.conn.Close()
	})
}

func (c *socketClient) readPump() {
	defer c.stop()
	c.conn.SetReadLimit(512)
	_ = c.conn.SetReadDeadline(time.Now().Add(pongWait))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(pongWait))
	})
	for {
		if _, _, err := c.conn.ReadMessage(); err != nil {
			return
		}
	}
}

func (c *socketClient) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.stop()
	}()
	for {
		select {
		case <-c.done:
			return
		case event := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteJSON(event); err != nil {
				return
			}
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
