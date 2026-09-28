// Command cexy-consumer-check builds a cexy client like a user's program. With
// CEXY_LIVE_TESTS=1 it also calls the public time endpoint.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/cexyio/cexy-go"
)

func main() {
	c, err := cexy.New(cexy.Options{})
	if err != nil {
		log.Fatalf("building the client: %v", err)
	}
	if cexy.Version == "" {
		log.Fatal("cexy.Version is empty")
	}
	if os.Getenv("CEXY_LIVE_TESTS") != "1" {
		fmt.Printf("cexy-go %s: client built (set CEXY_LIVE_TESTS=1 for a live call)\n", cexy.Version)
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	now, err := c.Time(ctx)
	if err != nil {
		log.Fatalf("live time call: %v", err)
	}
	fmt.Printf("cexy-go %s: live time call ok: %s\n", cexy.Version, now.Iso)
}
