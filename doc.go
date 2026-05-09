package weed

import (
	"log/slog"

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
		store:      store,
		logger:     logger,
		components: []abstract.Spark{},
	}
}
