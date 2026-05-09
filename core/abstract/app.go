package abstract

type App interface {
	StoreGet(k string) (any, bool)
	StoreSet(k string, v any) error
}
type Func func(App) error
