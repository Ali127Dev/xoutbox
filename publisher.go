package xoutbox

import "context"

type Publisher[T comparable] interface {
	Publish(ctx context.Context, event Event[T]) error
}
