package memtable

import (
	"lsmtree/kv"
)

// 不可变memtable
type ImmemtableOp interface {
	Search(key string) (kv.Kv, kv.SearchResult)
	GetValues() []kv.Kv
	GetIndex() string
}

func New(m MemtableOp) ImmemtableOp {
	return m
}
