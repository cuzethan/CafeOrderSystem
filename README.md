# Cafe Order System

My brother and I have recently into makeing some homeade drinks at home, and we wanted to emulate the ordering system in kiosks. I figured it would be pretty cool if I can build software to make it actually work. Because AI can write a lot of my code pretty easily, I spent a lot of time on the designing process. Hope this is pretty cool for you guys :D

-Ethan, written by hand

## Stack:

Golang, PostgreSQL, RabbitMQ (planned), WebSockets, Docker, Prometheus.

## Flow

1. Kiosk `GET /menu` — load available items from Postgres.
2. Kiosk `POST /orders` with `Idempotency-Key` — validate cart, save order as `pending`, return `202`.
3. Worker consumes `order.created`, moves the order to `accepted`, and publishes the order id on `order.updated`.
4. The API consumes `order.updated` and broadcasts it on the in-memory kitchen hub. Clients connect at `GET /ws/kitchen` (`orders.snapshot` on connect, then `order.updated`).
5. Staff `PATCH /orders/{id}/status` to advance: `accepted` → `preparing` → `ready` → `completed` (or cancel early).

Duplicate `Idempotency-Key` values return the same order and do not create a second one. If that order is still `pending`, the API publishes it again.

A missing order or an illegal status change is dead-lettered immediately. Any other failure is retried up to five times, then sent to `order.created.dlq` or `order.updated.dlq`.

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
