package weed

import (
	"log/slog"
	"os"

	"github.com/wdvn/kit/core/abstract"
	"github.com/wdvn/weed"
)

type api struct {
	sv     *weed.App
	logger *slog.Logger
}

func (t *api) Init(store abstract.Store) error {
	v, _ := store.Get("logger")
	t.logger = v.(*slog.Logger)
	return nil
}

func (t *api) Start() error {
	t.logger.Info("http://localhost:8080")
	return t.sv.Serve(":8080")
}

func (app *App) AddWeedServer(a *weed.App) {
	weedApi := &api{
		sv: a,
	}
	app.AddComponent(weedApi)
}

func New() *App {
	store := NewAppStore()
	logger := slog.Default()
	store.Set("logger", logger)
	return &App{
		store:       store,
		logger:      logger,
		components:  []abstract.Spark{},
		ch:          make(chan error, 16),
		signals:     defaultSignals,
		StopTimeout: defaultStopTimeout,
	}
}

// WithSignals overrides the termination signals the app listens for. Useful in
// tests (to drive a stop without killing the test process) and for daemons that
// want to react to SIGHUP as well.
func (app *App) WithSignals(sigs ...os.Signal) *App {
	app.signals = sigs
	return app
}
