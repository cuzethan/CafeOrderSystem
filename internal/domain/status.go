package domain

import "fmt"

// Status is the lifecycle state of an order.
type Status string

const (
	StatusPending   Status = "pending"
	StatusAccepted  Status = "accepted"
	StatusPreparing Status = "preparing"
	StatusReady     Status = "ready"
	StatusCompleted Status = "completed"
	StatusCancelled Status = "cancelled"
)

var allowedTransitions = map[Status]map[Status]struct{}{
	StatusPending: {
		StatusAccepted:  {},
		StatusCancelled: {},
	},
	StatusAccepted: {
		StatusPreparing: {},
		StatusCancelled: {},
	},
	StatusPreparing: {
		StatusReady: {},
	},
	StatusReady: {
		StatusCompleted: {},
	},
}

// CanTransition reports whether moving from -> to is a valid status change.
func CanTransition(from, to Status) error {
	next, ok := allowedTransitions[from]
	if !ok {
		return fmt.Errorf("cannot transition from %s to %s", from, to)
	}
	if _, ok := next[to]; !ok {
		return fmt.Errorf("cannot transition from %s to %s", from, to)
	}
	return nil
}
