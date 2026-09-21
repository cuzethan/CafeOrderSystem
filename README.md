# Cafe Order System

Go backend for cafe kiosk orders. Stack: `net/http`, PostgreSQL, RabbitMQ (planned), WebSockets, Docker, Prometheus.

## Flow

1. Kiosk `GET /menu` — load available items from Postgres.
2. Kiosk `POST /orders` with `Idempotency-Key` — validate cart, save order as `pending`, return `202`.
3. Worker (next) consumes the queue message and moves the order to `accepted`.
4. Kitchen clients get live updates from the in-memory WebSocket hub (`order.updated` / snapshot on connect).
5. Staff `PATCH /orders/{id}/status` to advance: `accepted` → `preparing` → `ready` → `completed` (or cancel early).

Duplicate `Idempotency-Key` values return the same order and do not create a second one.

## Run

```bash
docker compose up --build
```

- API: `http://localhost:8080`
- Prometheus: `http://localhost:9090`
- RabbitMQ UI: `http://localhost:15672` (guest/guest)

```bash
go test ./...
```

## License

MIT
