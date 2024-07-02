package sstable

import (
	"sync"

	"lsmtree/kv"
	"lsmtree/memtable"
)

// 默认实现是tableTree，todo 后续可以使用read through的方式增加cache的实现
type TableTreeOp interface {
	Search(key string) (kv.Kv, kv.SearchResult)
	Insert(imm memtable.ImmemtableOp) error
	CheckCompactLevels() []int
	CompactLevel(level int) error
}

func BuildTableTree(path string) TableTreeOp {
	// todo 从path读取所有sst文件，构建一个tableTree

}

/*
SSTable 文件由 {level}.{index}.db 组成
其中，索引越大，表示其文件越新。
*/

// TableTree 以层次结构去管理大量sstable
type TableTree struct {
	levels []*tableNode // 存储N层 sstable链表
	lock   sync.Locker
}

// sstable链表
type tableNode struct {
	index int
	table *SsTable
	next  *tableNode
}

func (t TableTree) Search(key string) (kv.Kv, kv.SearchResult) {
	//TODO implement me
	panic("implement me")
}

func (t TableTree) Insert(imm memtable.ImmemtableOp) error {
	//TODO implement me
	panic("implement me")
}

func (t TableTree) CheckCompactLevels() []int {
	//TODO implement me
	panic("implement me")
}

func (t TableTree) CompactLevel(level int) error {
	//TODO implement me
	panic("implement me")
}
