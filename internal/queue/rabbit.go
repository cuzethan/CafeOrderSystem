package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	// OrderCreatedQueue is the durable queue of order IDs waiting for ProcessOrder.
	OrderCreatedQueue = "order.created"
	// OrderCreatedDLQ holds order.created messages that will not succeed on retry.
	OrderCreatedDLQ = "order.created.dlq"
	// OrderUpdatedQueue is the durable queue of order IDs the API broadcasts to kitchen clients.
	// The worker publishes here after accept; the hub lives in the API process.
	OrderUpdatedQueue = "order.updated"
	// OrderUpdatedDLQ holds order.updated messages that will not succeed on retry.
	OrderUpdatedDLQ = "order.updated.dlq"
	// MaxDeliveries is how many times a job is attempted before it is dead-lettered.
	MaxDeliveries = 5
)

// Publisher sends order jobs to RabbitMQ.
type Publisher struct {
	conn     *amqp.Connection
	ch       *amqp.Channel
	confirms <-chan amqp.Confirmation
	mu       sync.Mutex
}

// NewPublisher dials RabbitMQ, declares the order queues, and enables publisher confirms.
func NewPublisher(url string) (*Publisher, error) {
	conn, ch, err := dial(url)
	if err != nil {
		return nil, err
	}
	if err := ch.Confirm(false); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("rabbitmq confirms: %w", err)
	}
	return &Publisher{
		conn:     conn,
		ch:       ch,
		confirms: ch.NotifyPublish(make(chan amqp.Confirmation, 1)),
	}, nil
}

// PublishOrderCreated publishes the order ID and waits for a broker confirm.
func (p *Publisher) PublishOrderCreated(orderID string) error {
	return p.publish(OrderCreatedQueue, orderID)
}

// PublishOrderUpdated publishes an accepted order ID for the API to broadcast.
func (p *Publisher) PublishOrderUpdated(orderID string) error {
	return p.publish(OrderUpdatedQueue, orderID)
}

func (p *Publisher) publish(queueName, body string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	pubCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := p.ch.PublishWithContext(pubCtx, "", queueName, false, false, amqp.Publishing{
		ContentType:  "text/plain",
		DeliveryMode: amqp.Persistent,
		Body:         []byte(body),
	})
	if err != nil {
		return fmt.Errorf("publish %s: %w", queueName, err)
	}

	select {
	case c, ok := <-p.confirms:
		if !ok {
			return fmt.Errorf("rabbitmq confirm channel closed")
		}
		if !c.Ack {
			return fmt.Errorf("rabbitmq nacked %s %s", queueName, body)
		}
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("rabbitmq confirm timeout")
	}
}

// QueueDepth returns how many messages are waiting on a declared queue.
func (p *Publisher) QueueDepth(name string) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	q, err := p.ch.QueueInspect(name)
	if err != nil {
		return 0, fmt.Errorf("inspect %s: %w", name, err)
	}
	return q.Messages, nil
}

// Close releases the channel and connection.
func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return closeConn(p.ch, p.conn)
}

// Consumer reads jobs from a RabbitMQ queue.
type Consumer struct {
	conn *amqp.Connection
	ch   *amqp.Channel
	msgs <-chan amqp.Delivery
}

// NewConsumer dials RabbitMQ and starts consuming queueName.
func NewConsumer(url, queueName string) (*Consumer, error) {
	conn, ch, err := dial(url)
	if err != nil {
		return nil, err
	}
	if err := ch.Qos(1, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("rabbitmq qos: %w", err)
	}
	msgs, err := ch.Consume(queueName, "cafe-"+queueName, false, false, false, false, nil)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("rabbitmq consume: %w", err)
	}
	return &Consumer{conn: conn, ch: ch, msgs: msgs}, nil
}

// Deliveries returns the message stream.
func (c *Consumer) Deliveries() <-chan amqp.Delivery {
	return c.msgs
}

// Close releases the channel and connection.
func (c *Consumer) Close() error {
	return closeConn(c.ch, c.conn)
}

func dial(url string) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, fmt.Errorf("dial rabbitmq: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("rabbitmq channel: %w", err)
	}
	if err := declareQueues(ch); err != nil {
		ch.Close()
		conn.Close()
		return nil, nil, err
	}
	return conn, ch, nil
}

// declareQueues creates each work queue as a quorum queue with a dead-letter
// queue. A nack without requeue lands in the DLQ. Transient failures may
// requeue, and the broker stops that after MaxDeliveries.
func declareQueues(ch *amqp.Channel) error {
	pairs := []struct{ work, dlq string }{
		{OrderCreatedQueue, OrderCreatedDLQ},
		{OrderUpdatedQueue, OrderUpdatedDLQ},
	}
	for _, pair := range pairs {
		if _, err := ch.QueueDeclare(pair.dlq, true, false, false, false, nil); err != nil {
			return fmt.Errorf("declare queue %s: %w", pair.dlq, err)
		}
		args := amqp.Table{
			"x-queue-type":              "quorum",
			"x-delivery-limit":          int32(MaxDeliveries),
			"x-dead-letter-exchange":    "",
			"x-dead-letter-routing-key": pair.dlq,
		}
		if _, err := ch.QueueDeclare(pair.work, true, false, false, false, args); err != nil {
			return fmt.Errorf("declare queue %s: %w", pair.work, err)
		}
	}
	return nil
}

// DeliveryAttempt reports which try this delivery is, starting at 1.
// Quorum queues set x-delivery-count to the number of previous attempts.
func DeliveryAttempt(d amqp.Delivery) int {
	if d.Headers == nil {
		return 1
	}
	n, ok := headerInt(d.Headers["x-delivery-count"])
	if !ok {
		return 1
	}
	return n + 1
}

func headerInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int8:
		return int(n), true
	case int16:
		return int(n), true
	case int32:
		return int(n), true
	case int64:
		return int(n), true
	default:
		return 0, false
	}
}

func closeConn(ch *amqp.Channel, conn *amqp.Connection) error {
	var err error
	if ch != nil {
		err = ch.Close()
	}
	if conn != nil {
		if cerr := conn.Close(); err == nil {
			err = cerr
		}
	}
	return err
}
