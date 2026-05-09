package weed

import (
	"fmt"
	"log/slog"

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

// Run app
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

	//TODO: implement graceful shutdown
	err := <-app.ch
	if err != nil {
		panic(err)
	}
}
