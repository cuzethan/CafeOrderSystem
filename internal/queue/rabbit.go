package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// OrderCreatedQueue is the durable queue of order IDs waiting for ProcessOrder.
const OrderCreatedQueue = "order.created"

// Publisher sends order-created jobs to RabbitMQ.
type Publisher struct {
	conn     *amqp.Connection
	ch       *amqp.Channel
	confirms <-chan amqp.Confirmation
	mu       sync.Mutex
}

// NewPublisher dials RabbitMQ, declares the order queue, and enables publisher confirms.
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
	p.mu.Lock()
	defer p.mu.Unlock()

	pubCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := p.ch.PublishWithContext(pubCtx, "", OrderCreatedQueue, false, false, amqp.Publishing{
		ContentType:  "text/plain",
		DeliveryMode: amqp.Persistent,
		Body:         []byte(orderID),
	})
	if err != nil {
		return fmt.Errorf("publish order: %w", err)
	}

	select {
	case c, ok := <-p.confirms:
		if !ok {
			return fmt.Errorf("rabbitmq confirm channel closed")
		}
		if !c.Ack {
			return fmt.Errorf("rabbitmq nacked order %s", orderID)
		}
		return nil
	case <-time.After(5 * time.Second):
		return fmt.Errorf("rabbitmq confirm timeout")
	}
}

// Close releases the channel and connection.
func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return closeConn(p.ch, p.conn)
}

// Consumer reads order-created jobs from RabbitMQ.
type Consumer struct {
	conn *amqp.Connection
	ch   *amqp.Channel
	msgs <-chan amqp.Delivery
}

// NewConsumer dials RabbitMQ and starts consuming the order queue.
func NewConsumer(url string) (*Consumer, error) {
	conn, ch, err := dial(url)
	if err != nil {
		return nil, err
	}
	if err := ch.Qos(1, 0, false); err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("rabbitmq qos: %w", err)
	}
	msgs, err := ch.Consume(OrderCreatedQueue, "cafe-order-worker", false, false, false, false, nil)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, fmt.Errorf("rabbitmq consume: %w", err)
	}
	return &Consumer{conn: conn, ch: ch, msgs: msgs}, nil
}

// Deliveries returns the order-created message stream.
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
	if _, err := ch.QueueDeclare(OrderCreatedQueue, true, false, false, false, nil); err != nil {
		ch.Close()
		conn.Close()
		return nil, nil, fmt.Errorf("declare queue: %w", err)
	}
	return conn, ch, nil
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
