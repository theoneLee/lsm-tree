package sstable

import (
	"sync"
)

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

//todo
// 1.当程序启动时，需要读取目录中所有的 SSTable 文件到 TableTree 中
// 2.tabletree sstable的合并
// 3.tabletree sstable的插入
// 4.tabletree sstable的查找
