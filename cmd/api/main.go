package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

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

	hub := ws.NewHub()
	hub.SetSnapshotProvider(func() []service.Order {
		orders, err := store.ListOpenOrders(context.Background())
		if err != nil {
			return nil
		}
		return orders
	})

	// RabbitMQ publisher comes later; noop keeps Create working end-to-end with Postgres.
	orders := service.NewOrderService(store, store, queue.NoopPublisher{}, hub)
	api := &httpapi.Handler{Orders: orders, Menu: store, Ready: store}

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
