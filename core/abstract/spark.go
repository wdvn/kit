package abstract

type Spark interface {
	Init(store Store) error
	Start() error
}
