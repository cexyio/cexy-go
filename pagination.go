package cexy

import (
	"context"
	"iter"
)

// Page is one page of a cursor-paginated listing.
type Page[T any] struct {
	Items   []T  `json:"items"`
	HasMore bool `json:"has_more"`
	// Pass it back as Cursor to get the next page. Nil on the last page.
	NextCursor *string `json:"next_cursor,omitempty"`
}

// paginate walks a listing page by page, yielding items one at a time and fetching the next
// page lazily. It stops on the last page (has_more false or no next_cursor), after maxItems
// items when maxItems > 0, or at the first error (yielded once with a zero item).
func paginate[T any](ctx context.Context, start *string, maxItems int,
	fetch func(ctx context.Context, cursor *string) (*Page[T], error)) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		cursor := start
		seen := map[string]bool{}
		n := 0
		for {
			page, err := fetch(ctx, cursor)
			if err != nil {
				var zero T
				yield(zero, err)
				return
			}
			for _, item := range page.Items {
				if maxItems > 0 && n >= maxItems {
					return
				}
				if !yield(item, nil) {
					return
				}
				n++
			}
			if !page.HasMore || page.NextCursor == nil || *page.NextCursor == "" {
				return
			}
			next := *page.NextCursor
			if seen[next] { // defensive: a repeated cursor would loop forever
				return
			}
			seen[next] = true
			cursor = &next
		}
	}
}

// IterOption limits an iterator: WithMaxItems.
type IterOption func(*iterOptions)

type iterOptions struct {
	maxItems int
	call     []CallOption
}

// WithMaxItems stops the iterator after n items in total.
func WithMaxItems(n int) IterOption { return func(o *iterOptions) { o.maxItems = n } }

// WithCallOptions applies call options (timeout, retries) to every page request.
func WithCallOptions(opts ...CallOption) IterOption {
	return func(o *iterOptions) { o.call = append(o.call, opts...) }
}

func iterOpts(opts []IterOption) iterOptions {
	var o iterOptions
	for _, f := range opts {
		f(&o)
	}
	return o
}
