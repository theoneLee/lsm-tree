package db

import (
	"fmt"
	"log"
	"path"
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
	sst sstable.TableTreeOp
	imm []memtable.ImmemtableOp // 后续imm列表是从新到旧排序的。后续查找imm时直接顺序查找即可。

	lock   *sync.RWMutex // 保护memtable到immemtable，wal的删除，immemtable到sstable，sstable的合并。
	stopCh chan struct{}
}

// 程序启动时
func (d *Db) Init(dir string) *Db {
	// 构建tabletree
	d.sst = sstable.RestoreTableTree(path.Join(dir, "sst"))

	d.w = wal.New()
	d.mem, d.imm = d.w.Restore(path.Join(dir, "wal"))

	d.lock = &sync.RWMutex{}
	d.stopCh = make(chan struct{})
	// 触发后台进程
	d.DemonTask()
	return d
}

func (d *Db) Shutdown() {
	d.stopCh <- struct{}{}
	d.demonTask()
}

func (d *Db) SetKv(val kv.Kv) error {
	err := d.w.Write(val)
	if err != nil {
		return err
	}
	d.mem.Set(val.Key, val.Value)
	if d.mem.CheckCap() {
		fmt.Printf("mem->imm,%v\n", d.mem.GetName())
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
		d.lock.Lock() // todo 这里锁的范围，应该加什么锁？
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
		log.Println("从mem获取key")
		return res
	}

	for _, imm := range d.imm { // 从新到旧遍历immemtable，然后进行二分查找
		res, result = imm.Search(key)
		if result != kv.None {
			log.Println("从imm获取key")
			return res
		}
	}

	res, result = d.sst.Search(key) //从tabletree上检索key
	if result != kv.None {
		log.Println("从sst获取key")
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
				fmt.Println("DemonTask start")
				err := d.demonTask()
				if err != nil {
					fmt.Printf("DemonTask err:%v", err)
				}
			case <-d.stopCh:
				fmt.Println("DemonTask finish.")
				return
			}
		}
	}()
}

func (d *Db) demonTask() error {
	d.lock.Lock()
	defer d.lock.Unlock()

	for _, imm := range d.imm {
		fmt.Printf("imm->sst,%v\n", imm.GetName())
		err := d.sst.Insert(imm) // 将imm转化为sst，放入tabletree管理
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

	levels := d.sst.CheckCompactLevels() // 检查是否触发sst合并
	if len(levels) == 0 {
		return nil
	}
	for _, level := range levels {
		fmt.Printf("compact sst[%v]->sst[%v] \n", level, level+1)
		err := d.sst.CompactLevel(level) // 将level的所有sst合并为一个sst后，放入level+1的tabletree上
		if err != nil {
			return err
		}
	}
	return nil
}

// todo 增加单测 demonTask GetKv DeleteKv Init
