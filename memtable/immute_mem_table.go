package memtable

import (
	"lsmtree/kv"
)

// 不可变memtable
type ImmemtableOp interface {
	Search(key string) (kv.Kv, kv.SearchResult)
	GetValues() []kv.Kv
	GetName() string
}

func NewImmemtable(m MemtableOp) ImmemtableOp {
	return m
}
