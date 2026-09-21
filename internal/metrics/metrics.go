package metrics

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
)

// Lightweight Prometheus-compatible metrics using only the Go standard library.

var ordersCreated atomic.Uint64

type counterKey struct {
	method, path, status string
}

var (
	httpMu       sync.Mutex
	httpRequests = map[counterKey]uint64{}
)

// OrdersCreated increments when an order is successfully created.
var OrdersCreated = ordersCreatedCounter{}

type ordersCreatedCounter struct{}

func (ordersCreatedCounter) Inc() { ordersCreated.Add(1) }

// HTTPRequests tracks HTTP request counts by method/path/status.
var HTTPRequests = httpCounter{}

type httpCounter struct{}

func (httpCounter) WithLabelValues(method, path, status string) labeledCounter {
	return labeledCounter{method: method, path: path, status: status}
}

type labeledCounter struct {
	method, path, status string
}

func (c labeledCounter) Inc() {
	httpMu.Lock()
	defer httpMu.Unlock()
	key := counterKey{c.method, c.path, c.status}
	httpRequests[key]++
}

// Handler exposes /metrics in Prometheus text exposition format.
func Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

		var b strings.Builder
		b.WriteString("# HELP cafe_orders_created_total Total orders successfully created.\n")
		b.WriteString("# TYPE cafe_orders_created_total counter\n")
		fmt.Fprintf(&b, "cafe_orders_created_total %d\n", ordersCreated.Load())

		b.WriteString("# HELP cafe_http_requests_total Total HTTP requests by method, path, and status.\n")
		b.WriteString("# TYPE cafe_http_requests_total counter\n")

		httpMu.Lock()
		for key, n := range httpRequests {
			fmt.Fprintf(&b,
				"cafe_http_requests_total{method=%q,path=%q,status=%q} %d\n",
				key.method, key.path, key.status, n,
			)
		}
		httpMu.Unlock()

		_, _ = w.Write([]byte(b.String()))
	})
}
