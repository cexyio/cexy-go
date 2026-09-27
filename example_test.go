package cexy_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	cexy "github.com/cexyio/cexy-go"
)

// Examples compile with the tests but do not run (no Output comments): they call the API.

func Example() {
	ctx := context.Background()
	c, err := cexy.New(cexy.Options{})
	if err != nil {
		log.Fatal(err)
	}
	markets, err := c.Markets.List(ctx)
	if err != nil {
		log.Fatal(err)
	}
	for _, m := range markets {
		fmt.Println(m.Symbol)
	}
}

func ExampleTradingService_PlaceOrder() {
	ctx := context.Background()
	c, err := cexy.New(cexy.Options{APIKey: os.Getenv("CEXY_API_KEY"), APISecret: os.Getenv("CEXY_API_SECRET")})
	if err != nil {
		log.Fatal(err)
	}
	placed, err := c.Trading.PlaceOrder(ctx, cexy.PlaceOrderRequest{
		Symbol:   "BTC/USDT",
		Side:     cexy.OrderSideBuy,
		Type:     cexy.OrderTypeLimit,
		Price:    cexy.Ptr(cexy.Amount("60000.00")),
		Quantity: cexy.Ptr(cexy.Amount("0.0010")),
	})
	var apiErr *cexy.APIError
	switch {
	case errors.As(err, &apiErr) && apiErr.Code == cexy.CodeInsufficientFunds:
		log.Println("not enough funds:", apiErr.Details)
	case err != nil:
		log.Fatal(err)
	default:
		fmt.Println(placed.Order.ID, placed.ClientOrderID, placed.Recovered)
	}
}

func ExampleTradingService_AllOrderHistory() {
	ctx := context.Background()
	c, _ := cexy.New(cexy.Options{APIKey: os.Getenv("CEXY_API_KEY"), APISecret: os.Getenv("CEXY_API_SECRET")})
	for order, err := range c.Trading.AllOrderHistory(ctx, &cexy.OrderHistoryParams{Symbol: cexy.Ptr("BTC/USDT")},
		cexy.WithMaxItems(500)) {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(order.ID, order.Status)
	}
}

func ExampleWebSocket_OrderBook() {
	ctx := context.Background()
	c, _ := cexy.New(cexy.Options{})
	ws, err := c.WebSocket(cexy.WSOptions{Handlers: cexy.WSHandlers{
		OnResync: func(cexy.ResyncReason) { log.Println("resync: refetch derived state") },
	}})
	if err != nil {
		log.Fatal(err)
	}
	if _, err := ws.Connect(ctx); err != nil {
		log.Fatal(err)
	}
	defer ws.Close()
	book, err := ws.OrderBook(ctx, "BTC/USDT", cexy.LiveOrderBookOptions{
		OnUpdate: func(b cexy.BookSnapshot) {
			bid, _, ask, _ := b.Best()
			fmt.Println(bid.Price, ask.Price, b.Stale)
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	defer book.Close()
}
