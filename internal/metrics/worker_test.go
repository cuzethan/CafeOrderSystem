package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWorkerHandlerExposesPendingQueueAndFailures(t *testing.T) {
	IncProcessFailure("dead_letter")

	h := WorkerHandler(
		func() (int, error) { return 3, nil },
		func(queue string) (int, error) {
			if queue == "order.created" {
				return 2, nil
			}
			return 1, nil
		},
		[]string{"order.created", "order.created.dlq"},
	)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		`cafe_orders_pending 3`,
		`cafe_queue_messages{queue="order.created"} 2`,
		`cafe_queue_messages{queue="order.created.dlq"} 1`,
		`cafe_worker_process_failures_total{result="dead_letter"}`,
		`cafe_worker_process_failures_total{result="requeue"} 0`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("body missing %q:\n%s", want, body)
		}
	}
}
