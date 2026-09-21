package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/cuzethan/CafeOrderSystem/internal/domain"
	"github.com/cuzethan/CafeOrderSystem/internal/service"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store implements service.OrderStore and service.MenuStore against Postgres.
type Store struct {
	pool *pgxpool.Pool
}

// New connects to Postgres using DATABASE_URL-style connection string.
func New(ctx context.Context, databaseURL string) (*Store, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("connect postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return &Store{pool: pool}, nil
}

// Close releases the connection pool.
func (s *Store) Close() {
	s.pool.Close()
}

// Ping checks database connectivity (for readiness probes).
func (s *Store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

func (s *Store) GetMenu() (map[string]domain.MenuItem, error) {
	rows, err := s.pool.Query(context.Background(), `
		SELECT id, name, price_cents, available
		FROM menu_items
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	menu := make(map[string]domain.MenuItem)
	for rows.Next() {
		var item domain.MenuItem
		if err := rows.Scan(&item.ID, &item.Name, &item.PriceCents, &item.Available); err != nil {
			return nil, err
		}
		menu[item.ID] = item
	}
	return menu, rows.Err()
}

func (s *Store) GetByIdempotencyKey(key string) (service.Order, bool, error) {
	var id string
	err := s.pool.QueryRow(context.Background(), `
		SELECT id FROM orders WHERE idempotency_key = $1
	`, key).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return service.Order{}, false, nil
	}
	if err != nil {
		return service.Order{}, false, err
	}
	return s.GetByID(id)
}

func (s *Store) GetByID(id string) (service.Order, bool, error) {
	var o service.Order
	err := s.pool.QueryRow(context.Background(), `
		SELECT id, kiosk_id, customer_name, status, idempotency_key, total_cents
		FROM orders WHERE id = $1
	`, id).Scan(&o.ID, &o.KioskID, &o.CustomerName, &o.Status, &o.IdempotencyKey, &o.TotalCents)
	if errors.Is(err, pgx.ErrNoRows) {
		return service.Order{}, false, nil
	}
	if err != nil {
		return service.Order{}, false, err
	}

	rows, err := s.pool.Query(context.Background(), `
		SELECT menu_item_id, quantity, unit_price_cents
		FROM order_items WHERE order_id = $1
		ORDER BY id
	`, id)
	if err != nil {
		return service.Order{}, false, err
	}
	defer rows.Close()

	for rows.Next() {
		var line service.OrderLine
		if err := rows.Scan(&line.MenuItemID, &line.Quantity, &line.UnitPriceCents); err != nil {
			return service.Order{}, false, err
		}
		o.Items = append(o.Items, line)
	}
	if err := rows.Err(); err != nil {
		return service.Order{}, false, err
	}
	return o, true, nil
}

func (s *Store) Create(order service.Order) error {
	ctx := context.Background()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO orders (id, kiosk_id, customer_name, status, idempotency_key, total_cents)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, order.ID, order.KioskID, order.CustomerName, order.Status, order.IdempotencyKey, order.TotalCents)
	if err != nil {
		return fmt.Errorf("insert order: %w", err)
	}

	for _, line := range order.Items {
		_, err = tx.Exec(ctx, `
			INSERT INTO order_items (order_id, menu_item_id, quantity, unit_price_cents)
			VALUES ($1, $2, $3, $4)
		`, order.ID, line.MenuItemID, line.Quantity, line.UnitPriceCents)
		if err != nil {
			return fmt.Errorf("insert order item: %w", err)
		}
	}
	return tx.Commit(ctx)
}

func (s *Store) UpdateStatus(id string, status domain.Status) error {
	tag, err := s.pool.Exec(context.Background(), `
		UPDATE orders SET status = $2, updated_at = NOW() WHERE id = $1
	`, id, status)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return service.ErrOrderNotFound
	}
	return nil
}

// ListOpenOrders returns non-terminal orders for kitchen snapshots.
func (s *Store) ListOpenOrders(ctx context.Context) ([]service.Order, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id FROM orders
		WHERE status NOT IN ('completed', 'cancelled')
		ORDER BY created_at
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []service.Order
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		o, ok, err := s.GetByID(id)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, o)
		}
	}
	return out, rows.Err()
}
