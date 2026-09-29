package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/cuzethan/CafeOrderSystem/internal/config"
	"github.com/cuzethan/CafeOrderSystem/internal/httpapi"
	"github.com/cuzethan/CafeOrderSystem/internal/queue"
	"github.com/cuzethan/CafeOrderSystem/internal/service"
	"github.com/cuzethan/CafeOrderSystem/internal/store/postgres"
	"github.com/cuzethan/CafeOrderSystem/internal/ws"
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

	publisher, err := waitForRabbit(cfg.RabbitMQURL)
	if err != nil {
		log.Fatalf("rabbitmq: %v", err)
	}
	defer publisher.Close()

	hub := ws.NewHub()
	hub.SetSnapshotProvider(func() []service.Order {
		orders, err := store.ListOpenOrders(context.Background())
		if err != nil {
			return nil
		}
		return orders
	})

	updates, err := waitForUpdates(cfg.RabbitMQURL)
	if err != nil {
		log.Fatalf("rabbitmq updates: %v", err)
	}
	defer updates.Close()

	orders := service.NewOrderService(store, store, publisher, hub)
	api := &httpapi.Handler{Orders: orders, Menu: store, Ready: store, Hub: hub}

	go consumeOrderUpdates(store, hub, updates)

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           api.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("api listening on %s", cfg.HTTPAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdownCtx)
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

func consumeOrderUpdates(store *postgres.Store, hub *ws.Hub, updates *queue.Consumer) {
	log.Printf("api consuming %s", queue.OrderUpdatedQueue)
	for delivery := range updates.Deliveries() {
		handleOrderUpdated(store, hub, delivery)
	}
}

func handleOrderUpdated(store *postgres.Store, hub *ws.Hub, delivery amqp.Delivery) {
	orderID := string(delivery.Body)
	order, ok, err := store.GetByID(orderID)
	if err != nil {
		requeue := queue.DeliveryAttempt(delivery) < queue.MaxDeliveries
		log.Printf("load %s for kitchen: %v (requeue=%v)", orderID, err, requeue)
		_ = delivery.Nack(false, requeue)
		if requeue {
			time.Sleep(time.Second)
		}
		return
	}
	if !ok {
		log.Printf("kitchen update for missing order %s", orderID)
		_ = delivery.Nack(false, false)
		return
	}
	if err := hub.NotifyOrderUpdated(order); err != nil {
		requeue := queue.DeliveryAttempt(delivery) < queue.MaxDeliveries
		log.Printf("broadcast %s: %v (requeue=%v)", orderID, err, requeue)
		_ = delivery.Nack(false, requeue)
		if requeue {
			time.Sleep(time.Second)
		}
		return
	}
	if ackErr := delivery.Ack(false); ackErr != nil {
		log.Printf("ack update %s: %v", orderID, ackErr)
	}
}

func waitForUpdates(url string) (*queue.Consumer, error) {
	var last error
	for i := 0; i < 30; i++ {
		consumer, err := queue.NewConsumer(url, queue.OrderUpdatedQueue)
		if err == nil {
			return consumer, nil
		}
		last = err
		log.Printf("waiting for rabbitmq updates: %v", err)
		time.Sleep(time.Second)
	}
	return nil, last
}

func waitForRabbit(url string) (*queue.Publisher, error) {
	var last error
	for i := 0; i < 30; i++ {
		publisher, err := queue.NewPublisher(url)
		if err == nil {
			return publisher, nil
		}
		last = err
		log.Printf("waiting for rabbitmq: %v", err)
		time.Sleep(time.Second)
	}
	return nil, last
}
