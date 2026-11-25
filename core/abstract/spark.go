package abstract

import "context"

// Spark is a runnable component of the app (HTTP server, queue worker, ...).
type Spark interface {
	Init(store Store) error
	Start() error
}

// Stoppable is implemented by components that need to release resources or
// drain in-flight work before the process exits.
//
// The app calls Stop once, after the first component has returned and before
// the process is allowed to die, so a component can e.g. finish the job it is
// holding, mark it as interrupted, close listeners or flush buffers.
//
// Implementations must be idempotent and must not block indefinitely: the app
// applies its own timeout and proceeds with exit even if Stop returns late.
type Stoppable interface {
	Stop(ctx context.Context) error
}
