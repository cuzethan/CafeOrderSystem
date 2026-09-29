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
	// OrderUpdatedQueue is the durable queue of order IDs the API broadcasts to kitchen clients.
	// The worker publishes here after accept; the hub lives in the API process.
	OrderUpdatedQueue = "order.updated"
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
	for _, name := range []string{OrderCreatedQueue, OrderUpdatedQueue} {
		if _, err := ch.QueueDeclare(name, true, false, false, false, nil); err != nil {
			ch.Close()
			conn.Close()
			return nil, nil, fmt.Errorf("declare queue %s: %w", name, err)
		}
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
