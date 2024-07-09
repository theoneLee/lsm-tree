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
	GetName() string
	CheckCap() bool     // 检查memtable是否超过阈值
	Merge(o MemtableOp) // 将o合并到self指针
}

func NewMemtable(path string) MemtableOp {
	return NewTree(path)
}
