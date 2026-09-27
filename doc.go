// Package cexy is the Go client for the CEXY.io REST and WebSocket API.
//
// Public market data needs no credentials:
//
//	c, err := cexy.New(cexy.Options{})
//	markets, err := c.Markets.List(ctx)
//
// Account, wallet reads and trading need an API key:
//
//	c, err := cexy.New(cexy.Options{APIKey: key, APISecret: secret})
//	balances, err := c.Account.Balances(ctx)
//
// Amounts are decimal strings ([Amount]), never floats. API errors are *[APIError] values;
// match their category with errors.Is (for example [ErrRateLimited]) and branch on
// [APIError.Code], never on the message.
//
// API keys can never withdraw or transfer funds, and this package has no such methods.
package cexy
