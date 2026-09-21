package domain

import "testing"

// TestAllowedTransitions checks the happy-path status moves kitchen staff (and
// the worker) are allowed to make.
//
// This is a "table-driven" test: we list many input pairs in a slice, then loop
// so we don't copy-paste the same assert over and over.
func TestAllowedTransitions(t *testing.T) {
	// Each row is one from -> to pair we expect to succeed.
	cases := []struct {
		from Status
		to   Status
	}{
		{StatusPending, StatusAccepted},
		{StatusPending, StatusCancelled},
		{StatusAccepted, StatusPreparing},
		{StatusAccepted, StatusCancelled},
		{StatusPreparing, StatusReady},
		{StatusReady, StatusCompleted},
	}

	// range gives us each case; tc is "test case" for that iteration.
	for _, tc := range cases {
		// CanTransition returns nil on success, or an error if the move is illegal.
		if err := CanTransition(tc.from, tc.to); err != nil {
			// t.Fatalf fails the test immediately and prints the message.
			t.Fatalf("expected %s -> %s to be allowed, got %v", tc.from, tc.to, err)
		}
	}
}

// TestDisallowedTransitions checks illegal jumps (e.g. pending straight to ready)
// and terminal states that must not move backward.
func TestDisallowedTransitions(t *testing.T) {
	cases := []struct {
		from Status
		to   Status
	}{
		{StatusPending, StatusReady},      // skip ahead
		{StatusPending, StatusPreparing},  // skip accepted
		{StatusPending, StatusCompleted},
		{StatusAccepted, StatusReady},
		{StatusAccepted, StatusCompleted},
		{StatusPreparing, StatusCompleted}, // skip ready
		{StatusPreparing, StatusCancelled}, // too late to cancel
		{StatusReady, StatusCancelled},
		{StatusCompleted, StatusPreparing}, // cannot reopen
		{StatusCancelled, StatusAccepted},
		{StatusPending, StatusPending}, // same status is not a transition
	}

	for _, tc := range cases {
		// Here we want an error. If err == nil, the bad transition was wrongly allowed.
		if err := CanTransition(tc.from, tc.to); err == nil {
			t.Fatalf("expected %s -> %s to be rejected", tc.from, tc.to)
		}
	}
}
