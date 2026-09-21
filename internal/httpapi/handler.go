package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/cuzethan/CafeOrderSystem/internal/domain"
	"github.com/cuzethan/CafeOrderSystem/internal/metrics"
	"github.com/cuzethan/CafeOrderSystem/internal/service"
)

// ReadyChecker reports whether dependent infrastructure is reachable.
type ReadyChecker interface {
	Ping(ctx context.Context) error
}

// Handler exposes REST endpoints for kiosks and kitchen status updates.
type Handler struct {
	Orders *service.OrderService
	Menu   service.MenuStore
	Ready  ReadyChecker
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", h.Health)
	mux.HandleFunc("GET /ready", h.Readyz)
	mux.Handle("GET /metrics", metrics.Handler())
	mux.HandleFunc("GET /menu", h.GetMenu)
	mux.HandleFunc("POST /orders", h.CreateOrder)
	mux.HandleFunc("GET /orders/{id}", h.GetOrder)
	mux.HandleFunc("PATCH /orders/{id}/status", h.UpdateStatus)
	return withMetrics(mux)
}

func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (h *Handler) Readyz(w http.ResponseWriter, r *http.Request) {
	if h.Ready != nil {
		if err := h.Ready.Ping(r.Context()); err != nil {
			writeError(w, http.StatusServiceUnavailable, "not ready")
			return
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

func (h *Handler) GetMenu(w http.ResponseWriter, r *http.Request) {
	menu, err := h.Menu.GetMenu()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load menu")
		return
	}
	items := make([]domain.MenuItem, 0, len(menu))
	for _, item := range menu {
		if item.Available {
			items = append(items, item)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items})
}

type createOrderRequest struct {
	KioskID      string                 `json:"kiosk_id"`
	CustomerName string                 `json:"customer_name"`
	Items        []domain.LineItemInput `json:"items"`
}

func (h *Handler) CreateOrder(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	var body createOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	order, err := h.Orders.Create(service.CreateOrderInput{
		KioskID:        body.KioskID,
		IdempotencyKey: key,
		CustomerName:   body.CustomerName,
		Items:          body.Items,
	})
	if err != nil {
		if errors.Is(err, service.ErrIdempotencyKeyRequired) {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	metrics.OrdersCreated.Inc()
	writeJSON(w, http.StatusAccepted, order)
}

func (h *Handler) GetOrder(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	order, err := h.Orders.Get(id)
	if err != nil {
		if errors.Is(err, service.ErrOrderNotFound) {
			writeError(w, http.StatusNotFound, "order not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to load order")
		return
	}
	writeJSON(w, http.StatusOK, order)
}

type updateStatusRequest struct {
	Status domain.Status `json:"status"`
}

func (h *Handler) UpdateStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var body updateStatusRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	order, err := h.Orders.UpdateStatus(id, body.Status)
	if err != nil {
		if errors.Is(err, service.ErrOrderNotFound) {
			writeError(w, http.StatusNotFound, "order not found")
			return
		}
		if strings.Contains(err.Error(), "cannot transition") {
			writeError(w, http.StatusConflict, err.Error())
			return
		}
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, order)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

type statusRecorder struct {
	http.ResponseWriter
	code int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.code = code
	s.ResponseWriter.WriteHeader(code)
}

func withMetrics(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(rec, r)
		path := r.Pattern
		if path == "" {
			path = r.URL.Path
		}
		metrics.HTTPRequests.WithLabelValues(r.Method, path, strconv.Itoa(rec.code)).Inc()
	})
}
