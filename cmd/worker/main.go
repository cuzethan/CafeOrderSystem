package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/cuzethan/CafeOrderSystem/internal/config"
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

	// The kitchen hub lives in the API process. This worker only persists accepted.
	orders := service.NewOrderService(store, store, queue.NoopPublisher{}, nil)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-stop
		_ = consumer.Close()
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

	requeue := shouldRequeue(err)
	log.Printf("process %s: %v (requeue=%v)", orderID, err, requeue)
	if nackErr := delivery.Nack(false, requeue); nackErr != nil {
		log.Printf("nack %s: %v", orderID, nackErr)
	}
	if requeue {
		time.Sleep(time.Second)
	}
}

func shouldRequeue(err error) bool {
	if errors.Is(err, service.ErrOrderNotFound) {
		return false
	}
	if strings.Contains(err.Error(), "cannot transition") {
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
		consumer, err := queue.NewConsumer(url)
		if err == nil {
			return consumer, nil
		}
		last = err
		log.Printf("waiting for rabbitmq: %v", err)
		time.Sleep(time.Second)
	}
	return nil, last
}
