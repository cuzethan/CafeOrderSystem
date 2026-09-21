package domain

import "testing"

// These tests cover ValidateLineItems: "is this cart OK to turn into an order?"
// We pass a fake menu map (id -> MenuItem) instead of talking to a real database.

// TestValidateLineItemsRejectsEmptyCart: no items at all should fail.
// nil is Go's "no slice" value — same idea as an empty cart here.
func TestValidateLineItemsRejectsEmptyCart(t *testing.T) {
	err := ValidateLineItems(nil, map[string]MenuItem{})
	if err == nil {
		t.Fatal("expected error for empty cart")
	}
}

// Quantity must be at least 1.
func TestValidateLineItemsRejectsZeroQuantity(t *testing.T) {
	// map[string]MenuItem is like a dictionary: key = item id, value = item details.
	menu := map[string]MenuItem{
		"latte": {ID: "latte", Name: "Latte", PriceCents: 450, Available: true},
	}
	err := ValidateLineItems([]LineItemInput{{MenuItemID: "latte", Quantity: 0}}, menu)
	if err == nil {
		t.Fatal("expected error for quantity 0")
	}
}

func TestValidateLineItemsRejectsNegativeQuantity(t *testing.T) {
	menu := map[string]MenuItem{
		"latte": {ID: "latte", Name: "Latte", PriceCents: 450, Available: true},
	}
	err := ValidateLineItems([]LineItemInput{{MenuItemID: "latte", Quantity: -1}}, menu)
	if err == nil {
		t.Fatal("expected error for negative quantity")
	}
}

// Ordering something that is not on the menu should fail.
func TestValidateLineItemsRejectsUnknownItem(t *testing.T) {
	err := ValidateLineItems(
		[]LineItemInput{{MenuItemID: "nope", Quantity: 1}},
		map[string]MenuItem{}, // empty menu — "nope" cannot exist
	)
	if err == nil {
		t.Fatal("expected error for unknown menu item")
	}
}

// Item exists but Available=false (sold out / disabled) should fail.
func TestValidateLineItemsRejectsUnavailableItem(t *testing.T) {
	menu := map[string]MenuItem{
		"soup": {ID: "soup", Name: "Soup", PriceCents: 600, Available: false},
	}
	err := ValidateLineItems([]LineItemInput{{MenuItemID: "soup", Quantity: 1}}, menu)
	if err == nil {
		t.Fatal("expected error for unavailable item")
	}
}

// Happy path: known, available items with positive quantities → no error.
func TestValidateLineItemsAcceptsValidCart(t *testing.T) {
	menu := map[string]MenuItem{
		"latte":  {ID: "latte", Name: "Latte", PriceCents: 450, Available: true},
		"muffin": {ID: "muffin", Name: "Muffin", PriceCents: 300, Available: true},
	}
	err := ValidateLineItems([]LineItemInput{
		{MenuItemID: "latte", Quantity: 2},
		{MenuItemID: "muffin", Quantity: 1},
	}, menu)
	if err != nil {
		t.Fatalf("expected valid cart, got %v", err)
	}
}
