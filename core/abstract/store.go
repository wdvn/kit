package abstract

type Store interface {
	Get(k string) (any, bool)
	Set(k string, v any)
}
