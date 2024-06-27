package memtable

import (
	"lsmtree/kv"
)

type MemtableOp interface {
	Search(key string) (kv.Kv, kv.SearchResult)
	Set(key string, value []byte) (oldValue kv.Kv, hasOld bool)
	Delete(key string) (oldValue kv.Kv, hasOld bool)
	GetValues() []kv.Kv
}
