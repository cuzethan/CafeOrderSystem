package service

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/cuzethan/CafeOrderSystem/internal/domain"
)

var (
	ErrIdempotencyKeyRequired = errors.New("idempotency key is required")
	ErrOrderNotFound          = errors.New("order not found")
)

// IDGenerator creates unique order IDs.
type IDGenerator func() string

func defaultIDGenerator() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// CreateOrderInput is the kiosk create-order request.
type CreateOrderInput struct {
	KioskID        string
	IdempotencyKey string
	CustomerName   string
	Items          []domain.LineItemInput
}

// OrderLine is a persisted line on an order.
type OrderLine struct {
	MenuItemID     string
	Quantity       int
	UnitPriceCents int
}

// Order is the service-layer order aggregate.
type Order struct {
	ID             string
	KioskID        string
	CustomerName   string
	IdempotencyKey string
	Status         domain.Status
	TotalCents     int
	Items          []OrderLine
}

// OrderStore persists orders.
type OrderStore interface {
	GetByIdempotencyKey(key string) (Order, bool, error)
	GetByID(id string) (Order, bool, error)
	Create(order Order) error
	UpdateStatus(id string, status domain.Status) error
}

// MenuStore loads the menu catalog.
type MenuStore interface {
	GetMenu() (map[string]domain.MenuItem, error)
}

// OrderPublisher enqueues order-created jobs.
type OrderPublisher interface {
	PublishOrderCreated(orderID string) error
}

// OrderNotifier pushes live updates (e.g. kitchen WebSocket hub).
type OrderNotifier interface {
	NotifyOrderUpdated(order Order) error
}

// OrderService handles order creation and related use cases.
type OrderService struct {
	orders   OrderStore
	menu     MenuStore
	pub      OrderPublisher
	notifier OrderNotifier
	newID    IDGenerator
}

// NewOrderService constructs an OrderService.
func NewOrderService(orders OrderStore, menu MenuStore, pub OrderPublisher, notifier OrderNotifier) *OrderService {
	if notifier == nil {
		notifier = noopNotifier{}
	}
	return &OrderService{
		orders:   orders,
		menu:     menu,
		pub:      pub,
		notifier: notifier,
		newID:    defaultIDGenerator,
	}
}

// Create validates, persists a pending order, and publishes a process job.
// Duplicate IdempotencyKey returns the existing order without re-publishing.
func (s *OrderService) Create(in CreateOrderInput) (Order, error) {
	if in.IdempotencyKey == "" {
		return Order{}, ErrIdempotencyKeyRequired
	}

	if existing, ok, err := s.orders.GetByIdempotencyKey(in.IdempotencyKey); err != nil {
		return Order{}, err
	} else if ok {
		return existing, nil
	}

	menu, err := s.menu.GetMenu()
	if err != nil {
		return Order{}, err
	}
	if err := domain.ValidateLineItems(in.Items, menu); err != nil {
		return Order{}, err
	}

	lines := make([]OrderLine, 0, len(in.Items))
	total := 0
	for _, item := range in.Items {
		mi := menu[item.MenuItemID]
		lines = append(lines, OrderLine{
			MenuItemID:     item.MenuItemID,
			Quantity:       item.Quantity,
			UnitPriceCents: mi.PriceCents,
		})
		total += mi.PriceCents * item.Quantity
	}

	order := Order{
		ID:             s.newID(),
		KioskID:        in.KioskID,
		CustomerName:   in.CustomerName,
		IdempotencyKey: in.IdempotencyKey,
		Status:         domain.StatusPending,
		TotalCents:     total,
		Items:          lines,
	}

	if err := s.orders.Create(order); err != nil {
		return Order{}, fmt.Errorf("create order: %w", err)
	}
	if err := s.pub.PublishOrderCreated(order.ID); err != nil {
		return Order{}, fmt.Errorf("publish order: %w", err)
	}
	return order, nil
}

// ProcessOrder is the worker entrypoint: pending -> accepted, then notify kitchen.
// Already-accepted orders are treated as success (idempotent redelivery).
func (s *OrderService) ProcessOrder(orderID string) error {
	order, ok, err := s.orders.GetByID(orderID)
	if err != nil {
		return err
	}
	if !ok {
		return ErrOrderNotFound
	}

	if order.Status == domain.StatusAccepted {
		return nil
	}

	if err := domain.CanTransition(order.Status, domain.StatusAccepted); err != nil {
		return err
	}
	if err := s.orders.UpdateStatus(orderID, domain.StatusAccepted); err != nil {
		return fmt.Errorf("update status: %w", err)
	}
	order.Status = domain.StatusAccepted
	if err := s.notifier.NotifyOrderUpdated(order); err != nil {
		return fmt.Errorf("notify: %w", err)
	}
	return nil
}

type noopNotifier struct{}

func (noopNotifier) NotifyOrderUpdated(Order) error { return nil }
