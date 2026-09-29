package service

import (
	"os"
	"testing"
	"time"

	"github.com/cuzethan/CafeOrderSystem/internal/domain"
	"github.com/cuzethan/CafeOrderSystem/internal/queue"
)

// TestCreatePublishesToRabbitMQ dials a real broker. The other Create tests
// use a fake publisher, so they still pass if Create stops calling Publish.
func TestCreatePublishesToRabbitMQ(t *testing.T) {
	url := os.Getenv("RABBITMQ_URL")
	if url == "" {
		t.Skip("RABBITMQ_URL is not set")
	}

	pub := waitForPublisher(t, url)
	t.Cleanup(func() { _ = pub.Close() })

	consumer, err := queue.NewConsumer(url, queue.OrderCreatedQueue)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	t.Cleanup(func() { _ = consumer.Close() })

	store := &fakeOrderStore{byKey: map[string]Order{}, byID: map[string]Order{}}
	menu := &fakeMenuStore{items: map[string]domain.MenuItem{
		"latte": {ID: "latte", Name: "Latte", PriceCents: 450, Available: true},
	}}
	svc := NewOrderService(store, menu, pub, nil)

	order, err := svc.Create(CreateOrderInput{
		KioskID:        "kiosk-ci",
		IdempotencyKey: "ci-broker",
		Items:          []domain.LineItemInput{{MenuItemID: "latte", Quantity: 1}},
	})
	if err != nil {
		t.Fatalf("Create: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for {
		select {
		case delivery, ok := <-consumer.Deliveries():
			if !ok {
				t.Fatal("consumer closed before the order id arrived")
			}
			got := string(delivery.Body)
			if got != order.ID {
				// Prefetch is 1. Put an unrelated message back so this test
				// can read the one Create just published.
				if err := delivery.Nack(false, true); err != nil {
					t.Fatalf("nack: %v", err)
				}
				continue
			}
			if err := delivery.Ack(false); err != nil {
				t.Fatalf("ack: %v", err)
			}
			return
		case <-deadline:
			t.Fatalf("order.created never received %s", order.ID)
		}
	}
}

func waitForPublisher(t *testing.T, url string) *queue.Publisher {
	t.Helper()
	var last error
	for i := 0; i < 20; i++ {
		pub, err := queue.NewPublisher(url)
		if err == nil {
			return pub
		}
		last = err
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("publisher: %v", last)
	return nil
}
