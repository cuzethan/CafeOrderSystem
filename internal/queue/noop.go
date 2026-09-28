package queue

// NoopPublisher drops publish calls. Tests and callers without a broker use it.
type NoopPublisher struct{}

func (NoopPublisher) PublishOrderCreated(orderID string) error {
	return nil
}
