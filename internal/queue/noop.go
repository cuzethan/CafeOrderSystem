package queue

// NoopPublisher is a stand-in until RabbitMQ publishing is wired.
// Create still succeeds; jobs are simply not enqueued yet.
type NoopPublisher struct{}

func (NoopPublisher) PublishOrderCreated(orderID string) error {
	return nil
}
