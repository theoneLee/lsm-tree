package db

import (
	"fmt"
	"sync"
	"time"

	"lsmtree/kv"
	"lsmtree/memtable"
	"lsmtree/sstable"
	"lsmtree/wal"
)

type Db struct {
	mem memtable.MemtableOp
	w   *wal.Wal
	sst sstable.TableTree
	imm []memtable.ImmemtableOp //todo 后续imm列表是从新到旧排序的。后续查找imm时直接顺序查找即可。

	lock *sync.RWMutex //todo 保护memtable到immemtable，wal的删除，immemtable到sstable，sstable的合并。
}

// 程序启动时
func (d *Db) Init(dir string) *Db {
	// 构建tabletree
	d.sst = sstable.BuildTableTree()

	d.w = wal.New()
	d.mem, d.imm = d.w.Restore(dir)

	d.lock = &sync.RWMutex{}
	// 触发后台进程
	d.DemonTask()
	return d
}

func (d *Db) SetKv(val kv.Kv) error {
	err := d.w.Write(val)
	if err != nil {
		return err
	}
	d.mem.Set(val.Key, val.Value)
	if d.mem.CheckCap() {
		d.lock.Lock()
		d.w = d.w.Reset()
		d.imm = append(d.imm, memtable.NewImmemtable(d.mem))
		d.mem = memtable.NewMemtable(d.w.GetPath())
		d.lock.Unlock()
	}
	return nil
}

func (d *Db) DeleteKv(val kv.Kv) error {
	err := d.w.Write(val)
	if err != nil {
		return err
	}
	d.mem.Delete(val.Key)
	if d.mem.CheckCap() { // 如果memtable达到阈值，形成immemtable
		d.lock.Lock()
		d.w = d.w.Reset()
		d.imm = append(d.imm, memtable.NewImmemtable(d.mem))
		d.mem = memtable.NewMemtable(d.w.GetPath())
		d.lock.Unlock()
	}
	return nil
}

func (d *Db) GetKv(key string) kv.Kv {
	d.lock.RLock()
	defer d.lock.RUnlock()
	res, result := d.mem.Search(key)
	if result != kv.None {
		return res
	}

	for _, imm := range d.imm { // 从新到旧遍历immemtable，然后进行二分查找
		res, result = imm.Search(key)
		if result != kv.None {
			return res
		}
	}

	res, result = d.sst.Search(key)
	if result != kv.None {
		return res
	}
	return kv.Kv{}
}

// 后台进程
func (d *Db) DemonTask() {
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		for {
			select {
			case <-ticker.C:
				err := d.demonTask()
				if err != nil {
					fmt.Printf("DemonTask err:%v", err)
				}
			}
		}
	}()
}

func (d *Db) demonTask() error {
	d.lock.Lock()
	defer d.lock.Unlock()

	for _, imm := range d.imm {
		err := d.sst.Insert(imm)
		if err != nil {
			return err
		}
		// 删除imm的wal
		err = d.w.Delete(imm.GetName())
		if err != nil {
			return err
		}
	}
	//删除 imm
	d.imm = []memtable.ImmemtableOp{}

	levels := d.sst.CheckCompactLevels()
	if len(levels) == 0 {
		return nil
	}
	for _, level := range levels {
		err := d.sst.CompactLevel(level)
		if err != nil {
			return err
		}
	}
	return nil
}
