package sstable

import (
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
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

func RestoreTableTree(dir string) TableTreeOp {
	// 从dir读取所有sst文件，构建一个tableTree
	tree := &TableTree{lock: &sync.Mutex{}, sstDir: dir}
	tree.lock.Lock()
	defer tree.lock.Unlock()

	sstPathList := getSstPathList2(dir) // 返回顺序需要排序 0.1.db 1.1.db 1.2.db 2.1.db
	for _, sstPath := range sstPathList {
		level, index := parseSstPath(dir, sstPath)
		_ = index
		sst := NewSst(sstPath)
		//fmt.Println("level:", level)

		// 构建sst，放入tree
		if len(tree.levels) > level {
			tree.levels[level].table = append(tree.levels[level].table, sst)
		} else {
			for i := 0; i < level; i++ { // 如果是1.0.db这种情况，需要在tree上先新增level为0的tableNode
				node := &tableNode{
					level: level,
					table: []SstOp{},
				}
				tree.levels = append(tree.levels, node)
			}
			node := &tableNode{
				level: level,
				table: []SstOp{sst},
			}
			tree.levels = append(tree.levels, node)
		}
	}
	return tree
}

func getSstPathList(dir string) []string {
	files, err := os.ReadDir(dir)
	if err != nil {
		panic(err)
	}
	type item struct {
		level int
		index int
		path  string
	}
	var list []item
	for _, file := range files {
		name := file.Name()
		level, index := parseSstPath(dir, name)
		list = append(list, item{
			level: level,
			index: index,
			path:  path.Join(dir, name),
		})
	}
	sort.Slice(list, func(i, j int) bool {
		if list[i].level < list[j].level {
			return true
		}
		if list[i].level > list[j].level {
			return false
		}
		// 到这里说明level相等
		if list[i].index < list[j].index {
			return true
		}
		if list[i].index > list[j].index {
			return false
		}
		return true
	})
	strs := []string{}
	for _, i := range list {
		strs = append(strs, i.path)
	}
	return strs
}

func getSstPathList2(dir string) []string {
	files, err := os.ReadDir(dir)
	if err != nil {
		if strings.Contains(err.Error(), "no such file or directory") {
			return nil
		}
		panic(err)
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].Name() < files[j].Name() // 可以使用字符串直接比较，因为命名规则符合字符串的比较大小的要求。
	})
	list := []string{}
	for _, file := range files {
		list = append(list, path.Join(dir, file.Name()))
	}
	return list
}

func parseSstPath(dir, sstPath string) (int, int) {
	_, sstPath = path.Split(sstPath) // 移除dir，预期是1.0.db这样的文件名
	list := strings.Split(sstPath, ".")
	if len(list) != 3 {
		panic(fmt.Sprintf("sstPath:%v 不符合{level}.{index}.db", sstPath))
	}
	level, err := strconv.Atoi(list[0])
	if err != nil {
		panic(err)
	}
	index, err := strconv.Atoi(list[1])
	if err != nil {
		panic(err)
	}
	return level, index
}

/*
SSTable 文件由 {level}.{index}.db 组成
其中，索引越大，表示其文件越新。
*/

// TableTree 以层次结构去管理大量sstable
type TableTree struct {
	levels []*tableNode // 存储N层 sstable链表
	lock   sync.Locker
	sstDir string
}

//// sstable链表
//type tableNode struct {
//	index int
//	table *SsTable
//	next  *tableNode
//}

type tableNode struct {
	level int
	table []SstOp
}

const sstFileSuffix = ".db"

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
	// 获取最大的level0的index，然后作为path
	var name string
	if len(t.levels) > 0 {
		index := len(t.levels[0].table)
		level := t.levels[0].level
		name = fmt.Sprintf("%v.%v%v", level, index, sstFileSuffix)
	} else {
		name = fmt.Sprintf("%v.%v%v", 0, 0, sstFileSuffix)
	}
	sstPath := path.Join(t.sstDir, name)
	os.MkdirAll(t.sstDir, 0755) //确保目录t.sstDir存在
	sst := NewSst(sstPath)
	err := sst.Encode(imm) //编码并写入sst.f
	if err != nil {
		return err
	}
	insertLevel := 0 // 不可变memtable始终会插入到第0层
	if len(t.levels) == 0 {
		node := &tableNode{
			level: 0,
			table: []SstOp{sst},
		}
		t.levels = append(t.levels, node)
		return nil
	}
	t.levels[insertLevel].table = append(t.levels[insertLevel].table, sst)
	return nil
}

// 每次允许的sstable个数，超过说明该层需要合并 //todo 这里如何重构？
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
		if len(sstList.table) > levelCountLimit[i] { // todo 这里判断标准是否合理？是否需要重构？
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

	// 获取level+1的长度作为index
	var name string
	if len(t.levels) > level+1 {
		index := len(t.levels[level+1].table)
		name = fmt.Sprintf("%v.%v%v", level+1, index, sstFileSuffix)
	} else {
		name = fmt.Sprintf("%v.%v%v", level+1, 0, sstFileSuffix)
	}
	sstPath := path.Join(t.sstDir, name)
	temp := NewSst(sstPath)
	tree := memtable.NewTree("")
	for i := 0; i < tableLen; i++ {
		sst := t.levels[level].table[i]
		o, err := sst.Decode()
		if err != nil {
			panic(err)
		}
		tree.Merge(o)
	}

	// tree encode为sst
	err := temp.Encode(tree) //编码并写入sst.f
	if err != nil {
		return err
	}

	//将temp作为下一个level的sst放入
	if len(t.levels) > level+1 {
		t.levels[level+1].table = append(t.levels[level+1].table, temp)
	} else {
		node := &tableNode{
			level: 0,
			table: []SstOp{temp},
		}
		t.levels = append(t.levels, node)
	}

	// 清理level层的sst。文件和内存
	for _, sst := range t.levels[level].table {
		err = sst.Delete()
		if err != nil {
			return err
		}
	}
	t.levels[level].table = nil

	return nil
}
