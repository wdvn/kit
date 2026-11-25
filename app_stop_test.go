package weed

import (
	"context"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/wdvn/kit/core/abstract"
)

// stoppableComponent records Start/Stop calls without needing a real server.
type stoppableComponent struct {
	started  atomic.Bool
	stopped  atomic.Bool
	stopErr  error
	stopBlk  time.Duration // how long Stop blocks, to exercise the timeout
	startErr error
	block    chan struct{} // if non-nil, Start blocks until closed
}

func (c *stoppableComponent) Init(abstract.Store) error { return nil }

func (c *stoppableComponent) Start() error {
	c.started.Store(true)
	if c.block != nil {
		<-c.block
	}
	return c.startErr
}

func (c *stoppableComponent) Stop(context.Context) error {
	if c.stopBlk > 0 {
		time.Sleep(c.stopBlk)
	}
	c.stopped.Store(true)
	return c.stopErr
}

// plainComponent implements Spark only: it must not break Run.
type plainComponent struct {
	startErr error
	block    chan struct{}
}

func (c *plainComponent) Init(abstract.Store) error { return nil }
func (c *plainComponent) Start() error {
	if c.block != nil {
		<-c.block
	}
	return c.startErr
}

func TestRunStopsStoppableComponentsOnSignal(t *testing.T) {
	c := &stoppableComponent{block: make(chan struct{})}
	app := New()
	app.WithSignals(syscall.SIGUSR1)
	app.StopTimeout = 2 * time.Second
	app.AddComponent(c)

	done := make(chan struct{})
	go func() {
		app.Run()
		close(done)
	}()

	waitFor(t, func() bool { return c.started.Load() })
	if err := syscall.Kill(os.Getpid(), syscall.SIGUSR1); err != nil {
		t.Fatalf("kill: %v", err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after signal")
	}
	if !c.stopped.Load() {
		t.Fatal("Stop was not called on signal")
	}
}

func TestRunStopsComponentsWhenOneReturns(t *testing.T) {
	exiting := &plainComponent{}
	other := &stoppableComponent{block: make(chan struct{})}
	app := New()
	app.StopTimeout = 2 * time.Second
	app.AddComponent(exiting)
	app.AddComponent(other)

	done := make(chan struct{})
	go func() {
		app.Run()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after a component returned")
	}
	if !other.stopped.Load() {
		t.Fatal("Stop was not called when another component returned")
	}
}

func TestRunRespectsStopTimeout(t *testing.T) {
	slow := &stoppableComponent{block: make(chan struct{}), stopBlk: 10 * time.Second}
	app := New()
	app.WithSignals(syscall.SIGUSR1)
	app.StopTimeout = 150 * time.Millisecond
	app.AddComponent(slow)

	done := make(chan struct{})
	var elapsed time.Duration
	go func() {
		start := time.Now()
		app.Run()
		elapsed = time.Since(start)
		close(done)
	}()

	waitFor(t, func() bool { return slow.started.Load() })
	if err := syscall.Kill(os.Getpid(), syscall.SIGUSR1); err != nil {
		t.Fatalf("kill: %v", err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return; StopTimeout was not enforced")
	}

	if elapsed > 2*time.Second {
		t.Fatalf("Run waited %s, expected it to give up near StopTimeout", elapsed)
	}
}

func TestRunWithoutStoppableComponentsDoesNotHang(t *testing.T) {
	app := New()
	app.AddComponent(&plainComponent{})

	done := make(chan struct{})
	go func() {
		app.Run()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Run hung without any Stoppable component")
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}
