package queue

import (
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestDeliveryAttemptStartsAtOne(t *testing.T) {
	if got := DeliveryAttempt(amqp.Delivery{}); got != 1 {
		t.Fatalf("attempt = %d, want 1", got)
	}
}

func TestDeliveryAttemptCountsPreviousTries(t *testing.T) {
	d := amqp.Delivery{Headers: amqp.Table{"x-delivery-count": int64(4)}}
	if got := DeliveryAttempt(d); got != 5 {
		t.Fatalf("attempt = %d, want 5", got)
	}
}
