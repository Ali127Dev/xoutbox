package xoutbox

import "context"

type Store[T comparable] interface {
	InsertEvent(ctx context.Context, event Event[T]) error
	FetchPending(ctx context.Context, limit int) ([]Event[T], error)
	MarkPublished(ctx context.Context, id T) error
	MarkFailed(ctx context.Context, id T, retryCount int) error
}
