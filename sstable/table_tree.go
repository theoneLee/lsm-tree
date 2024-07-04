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

func RestoreTableTree(path string) TableTreeOp {
	// todo 从path读取所有sst文件，构建一个tableTree
	tree := &TableTree{lock: &sync.Mutex{}}
	tree.lock.Lock()
	defer tree.lock.Unlock()

	sstPathList := get_sst_path_list() // 返回顺序需要排序 0.1.db 1.1.db 1.2.db 2.1.db
	for _, sstPath := range sstPathList {
		level, index := parse_sst_path(sstPath)
		sst := RestoreSst(sstPath, index)

		// 构建sst，放入tree
		if len(tree.levels) >= level {
			tree.levels[level].table = append(tree.levels[level].table, sst)
		} else {
			node := &tableNode{
				level: level,
				table: []*SsTable{sst},
			}
			tree.levels = append(tree.levels, node)
		}
	}
	return tree
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

//// sstable链表
//type tableNode struct {
//	index int
//	table *SsTable
//	next  *tableNode
//}

type tableNode struct {
	level int
	table []*SsTable
}

func (t *TableTree) Search(key string) (kv.Kv, kv.SearchResult) {
	// 优先先读新的sst。即level小，index大的
	for _, sstList := range t.levels {
		for i := len(sstList.table) - 1; i >= 0; i-- {
			sst := sstList.table[i]
			res, result := sst.Search(key) //todo search 时，先走布隆过滤器？ 然后解码后再读索引
			if result != kv.None {
				return res, result
			}
		}
	}
	return kv.Kv{}, kv.None
}

// 将imm转化为sst，放入tabletree管理
func (t *TableTree) Insert(imm memtable.ImmemtableOp) error {
	t.lock.Lock()
	defer t.lock.Unlock()
	sst := BuildSst()      // todo 参数
	err := sst.Encode(imm) // todo 编码并写入sst.f
	if err != nil {
		return err
	}
	insertLevel := 0 // 不可变memtable始终会插入到第0层
	if len(t.levels) == 0 {
		node := &tableNode{
			level: 0,
			table: []*SsTable{sst},
		}
		t.levels = append(t.levels, node)
		return nil
	}
	t.levels[insertLevel].table = append(t.levels[insertLevel].table, sst)
	return nil
}

// 每次允许的sstable个数，超过说明该层需要合并
var levelCountLimit = map[int]int{
	0: 10,
	1: 10,
	2: 10,
	3: 10,
	4: 10,
	5: 10,
	6: 10,
}

// 检查是否触发sst合并
func (t *TableTree) CheckCompactLevels() []int {
	// 检查每一层的个数是否超过阈值
	var list []int
	for i, sstList := range t.levels {
		if len(sstList.table) > levelCountLimit[i] {
			list = append(list, i)
		}
	}
	return list
}

// 将level的所有sst合并为一个sst后，放入level+1的tabletree上
func (t *TableTree) CompactLevel(level int) error {
	t.lock.Lock()
	defer t.lock.Unlock()

	if len(t.levels[level].table) == 0 {
		return nil
	}
	tableLen := len(t.levels[level].table)
	temp := &SsTable{ // todo 是否抽为new函数
		f:             nil,
		filePath:      "",
		tableMetaInfo: MetaInfo{},
		startPoints:   nil,
		lock:          nil,
	}
	for i := 0; i < tableLen; i++ {
		sst := t.levels[level].table[i]
		temp.Merge(sst) // todo 将sst的数据覆盖temp
	}
	// todo 将temp作为下一个level的sst放入
	if tableLen >= level+1 {
		t.levels[level+1].table = append(t.levels[level+1].table, temp)
	} else {
		node := &tableNode{
			level: 0,
			table: []*SsTable{temp},
		}
		t.levels = append(t.levels, node)
	}
	return nil
}
