package weed

import (
	"sync"

	"github.com/wdvn/kit/core/abstract"
)

type appStore struct {
	m *sync.Map
}

func (s *appStore) Get(k string) (any, bool) {
	return s.m.Load(k)
}

func (s *appStore) Set(k string, v any) {
	s.m.Store(k, v)
}
func (s *appStore) Del(k string) {
	s.m.Delete(k)
}

func NewAppStore() abstract.Store {
	return &appStore{
		m: &sync.Map{},
	}
}
