package weed

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/wdvn/kit/core/abstract"
	"github.com/wdvn/weed"
)

type App struct {
	sv          *weed.App
	store       abstract.Store
	initializer []abstract.Func
	logger      *slog.Logger
	components  []abstract.Spark
	ch          chan error

	// StopTimeout bounds how long a component may take to release its
	// resources once the app is asked to exit. Deployment sends SIGTERM and
	// docker gives 10s before SIGKILL, so the default stays clearly below it.
	StopTimeout time.Duration
	// signals receives the termination signals the app reacts to.
	signals []os.Signal
}

// AddInitialFunc init a function this function call before run app
func (app *App) AddInitialFunc(f abstract.Func) {
	err := f(app)
	if err != nil {
		panic(err)
	}
}

// StoreGet SGet - Get an item from app store
func (app *App) StoreGet(k string) (any, bool) {
	return app.store.Get(k)
}

// StoreSet - Set an item to store, throw error if key is exist
func (app *App) StoreSet(k string, v any) error {
	if _, ok := app.store.Get(k); ok {
		return fmt.Errorf("item %s already exists", k)
	}
	app.store.Set(k, v)
	return nil
}

// AddComponent - add component to run app
func (app *App) AddComponent(component abstract.Spark) {
	app.components = append(app.components, component)
}

// Run app.
//
// Run blocks until either a component returns or a termination signal
// (SIGINT/SIGTERM) is received. In both cases every component implementing
// abstract.Stoppable is stopped, bounded by StopTimeout, before Run returns.
// Previously the process died instantly on SIGTERM (container restart, deploy,
// Ctrl+C), so in-flight work was lost with no chance to record anything.
func (app *App) Run() {
	for _, fn := range app.initializer {
		if err := fn(app); err != nil {
			panic(err)
		}
	}
	for _, component := range app.components {
		err := component.Init(app.store)
		if err != nil {
			panic(err)
		}
	}

	for _, component := range app.components {
		go func() {
			app.ch <- component.Start()
		}()
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, app.signals...)
	defer signal.Stop(sigCh)

	select {
	case err := <-app.ch:
		if err != nil {
			app.logger.Error("component stopped with error", "error", err)
		}
	case sig := <-sigCh:
		app.logger.Info("termination signal received, stopping components", "signal", sig.String())
	}

	app.stopComponents()
}

// stopComponents calls Stop on every Stoppable component concurrently and
// waits for all of them or for StopTimeout, whichever comes first. It never
// returns an error: the process is going down either way, and a timeout must
// not turn a clean stop into a panic.
func (app *App) stopComponents() {
	var stoppable []abstract.Stoppable
	for _, component := range app.components {
		if s, ok := component.(abstract.Stoppable); ok {
			stoppable = append(stoppable, s)
		}
	}
	if len(stoppable) == 0 {
		return
	}

	timeout := app.StopTimeout
	if timeout <= 0 {
		timeout = defaultStopTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var wg sync.WaitGroup
	for _, s := range stoppable {
		wg.Add(1)
		go func(s abstract.Stoppable) {
			defer wg.Done()
			if err := s.Stop(ctx); err != nil {
				app.logger.Error("component stop failed", "error", err)
			}
		}(s)
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		app.logger.Info("all components stopped")
	case <-ctx.Done():
		app.logger.Warn("component stop timed out, exiting anyway", "timeout", timeout.String())
	}
}

// defaultStopTimeout leaves headroom under docker's 10s SIGTERM grace period.
const defaultStopTimeout = 8 * time.Second

// defaultSignals are the signals a container runtime or terminal sends to ask
// for a clean exit.
var defaultSignals = []os.Signal{syscall.SIGINT, syscall.SIGTERM}
