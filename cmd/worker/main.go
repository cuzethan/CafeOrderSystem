package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/cuzethan/CafeOrderSystem/internal/config"
	"github.com/cuzethan/CafeOrderSystem/internal/domain"
	"github.com/cuzethan/CafeOrderSystem/internal/metrics"
	"github.com/cuzethan/CafeOrderSystem/internal/queue"
	"github.com/cuzethan/CafeOrderSystem/internal/service"
	"github.com/cuzethan/CafeOrderSystem/internal/store/postgres"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx := context.Background()
	store, err := waitForPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("postgres: %v", err)
	}
	defer store.Close()

	consumer, err := waitForRabbit(cfg.RabbitMQURL)
	if err != nil {
		log.Fatalf("rabbitmq: %v", err)
	}
	defer consumer.Close()

	updates, err := waitForPublisher(cfg.RabbitMQURL)
	if err != nil {
		log.Fatalf("rabbitmq publisher: %v", err)
	}
	defer updates.Close()

	// The kitchen hub lives in the API process. Publish order.updated after accept
	// so the API can broadcast to connected displays.
	orders := service.NewOrderService(store, store, queue.NoopPublisher{}, kitchenNotifier{pub: updates})

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics.WorkerHandler(
		func() (int, error) { return store.CountPending(context.Background()) },
		updates.QueueDepth,
		[]string{queue.OrderCreatedQueue, queue.OrderCreatedDLQ},
	))
	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("worker metrics on %s", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("metrics: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-stop
		_ = consumer.Close()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Printf("worker consuming %s", queue.OrderCreatedQueue)
	for delivery := range consumer.Deliveries() {
		handle(orders, delivery)
	}
}

func handle(orders *service.OrderService, delivery amqp.Delivery) {
	orderID := string(delivery.Body)
	err := orders.ProcessOrder(orderID)
	if err == nil {
		if ackErr := delivery.Ack(false); ackErr != nil {
			log.Printf("ack %s: %v", orderID, ackErr)
		}
		return
	}

	requeue := shouldRequeue(err) && queue.DeliveryAttempt(delivery) < queue.MaxDeliveries
	if requeue {
		metrics.IncProcessFailure("requeue")
	} else {
		metrics.IncProcessFailure("dead_letter")
	}
	log.Printf("process %s: %v (requeue=%v)", orderID, err, requeue)
	if nackErr := delivery.Nack(false, requeue); nackErr != nil {
		log.Printf("nack %s: %v", orderID, nackErr)
	}
	if requeue {
		time.Sleep(time.Second)
	}
}

// shouldRequeue reports whether a processing error might succeed later.
// Missing orders and illegal transitions are dead-lettered immediately.
// Other errors, including database failures, are retried up to MaxDeliveries.
func shouldRequeue(err error) bool {
	if errors.Is(err, service.ErrOrderNotFound) {
		return false
	}
	if errors.Is(err, domain.ErrInvalidTransition) {
		return false
	}
	return true
}

func waitForPostgres(ctx context.Context, databaseURL string) (*postgres.Store, error) {
	var last error
	for i := 0; i < 30; i++ {
		store, err := postgres.New(ctx, databaseURL)
		if err == nil {
			return store, nil
		}
		last = err
		log.Printf("waiting for postgres: %v", err)
		time.Sleep(time.Second)
	}
	return nil, last
}

func waitForRabbit(url string) (*queue.Consumer, error) {
	var last error
	for i := 0; i < 30; i++ {
		consumer, err := queue.NewConsumer(url, queue.OrderCreatedQueue)
		if err == nil {
			return consumer, nil
		}
		last = err
		log.Printf("waiting for rabbitmq: %v", err)
		time.Sleep(time.Second)
	}
	return nil, last
}

func waitForPublisher(url string) (*queue.Publisher, error) {
	var last error
	for i := 0; i < 30; i++ {
		publisher, err := queue.NewPublisher(url)
		if err == nil {
			return publisher, nil
		}
		last = err
		log.Printf("waiting for rabbitmq publisher: %v", err)
		time.Sleep(time.Second)
	}
	return nil, last
}

// kitchenNotifier forwards an accepted order to the API over RabbitMQ.
// The API consumes that message and broadcasts on its in-memory hub.
type kitchenNotifier struct {
	pub *queue.Publisher
}

func (n kitchenNotifier) NotifyOrderUpdated(order service.Order) error {
	return n.pub.PublishOrderUpdated(order.ID)
}
