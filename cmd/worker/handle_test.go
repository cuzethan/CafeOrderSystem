package main

import (
	"errors"
	"fmt"
	"testing"

	"github.com/cuzethan/CafeOrderSystem/internal/domain"
	"github.com/cuzethan/CafeOrderSystem/internal/service"
)

func TestShouldRequeue(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{name: "missing order", err: service.ErrOrderNotFound, want: false},
		{name: "wrapped missing order", err: fmt.Errorf("load: %w", service.ErrOrderNotFound), want: false},
		{name: "illegal transition", err: domain.ErrInvalidTransition, want: false},
		{name: "wrapped transition", err: fmt.Errorf("%w: cannot transition from preparing to accepted", domain.ErrInvalidTransition), want: false},
		{name: "database error", err: errors.New("dial tcp: connection refused"), want: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := shouldRequeue(tc.err); got != tc.want {
				t.Fatalf("shouldRequeue(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
