package domain

import "fmt"

// MenuItem is a sellable cafe product.
type MenuItem struct {
	ID         string
	Name       string
	PriceCents int
	Available  bool
}

// LineItemInput is one line on a create-order request.
type LineItemInput struct {
	MenuItemID string
	Quantity   int
}

// ValidateLineItems checks cart contents against the current menu.
func ValidateLineItems(items []LineItemInput, menu map[string]MenuItem) error {
	if len(items) == 0 {
		return fmt.Errorf("cart must contain at least one item")
	}
	for _, item := range items {
		if item.Quantity < 1 {
			return fmt.Errorf("quantity must be at least 1 for item %q", item.MenuItemID)
		}
		mi, ok := menu[item.MenuItemID]
		if !ok {
			return fmt.Errorf("unknown menu item %q", item.MenuItemID)
		}
		if !mi.Available {
			return fmt.Errorf("menu item %q is unavailable", item.MenuItemID)
		}
	}
	return nil
}
