package memtable

import (
	"lsmtree/kv"
)

// todo 后续可以添加 红黑树/跳表实现
type MemtableOp interface {
	Search(key string) (kv.Kv, kv.SearchResult)
	Set(key string, value []byte) (oldValue kv.Kv, hasOld bool)
	Delete(key string) (oldValue kv.Kv, hasOld bool)
	GetValues() []kv.Kv
	GetIndex() string
}
