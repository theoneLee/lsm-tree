package wal

import (
	"bytes"
	"encoding/binary"
	"log"
	"os"
	"path"
	"sync"
	"time"

	"lsmtree/errs"
	"lsmtree/kv"
	"lsmtree/memtable"
)

/*
WAL 需要具备两种能力：

1，程序启动时，能够读取 WAL 文件的内容，恢复为内存表（二叉排序树）。

2，程序启动后，写入、删除操作内存表时，操作要写入到 WAL 文件中。(WAL记录的是所有的写操作，而不是记录内存表的状态)
*/

type Wal struct {
	name int64 // 从1开始。 name最大的为memtable wal，其余的为immemtable
	f    *os.File
	path string
	lock *sync.Mutex

	marsher kv.MarshalOp
}

// todo init wal。确定wal文件规则以及什么时候替换

func (w *Wal) Write(val kv.Kv) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	data, err := w.marsher.Marshal(val)
	if err != nil {
		return errs.Newf(errs.ErrWal, "err:%v", err)
	}

	//先写入一个 8 字节，再将 Key/Value 序列化写入。
	// [int64记录data长度, data]
	err = binary.Write(w.f, binary.LittleEndian, int64(len(data)))
	if err != nil {
		return errs.Newf(errs.ErrWal, "err:%v", err)
	}

	err = binary.Write(w.f, binary.LittleEndian, data)
	if err != nil {
		return errs.Newf(errs.ErrWal, "err:%v", err)
	}
	return nil
}

// todo 从wal文件恢复。
func (w *Wal) Init(dir string) memtable.MemtableOp {
	start := time.Now()
	defer func() {
		log.Println("Load wal cost:", time.Since(start))
	}()

	walPath := path.Join(dir, "wal.log")
	f, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	w.f = f
	w.path = walPath
	w.lock = &sync.Mutex{}
	w.marsher = kv.Json{}
	return w.loadToMemory()
}

// 从wal文件上还原为一个memtable
func (w *Wal) loadToMemory() memtable.MemtableOp {
	w.lock.Lock()
	defer w.lock.Unlock()

	info, _ := os.Stat(w.path)
	size := info.Size()
	tree := memtable.NewTree()
	//首先读取文件开头的 8 个字节，确定第一个元素的字节数量 n，然后将 8 ~ (8+n) 范围中的二进制数据反序列化为treeNode
	//（根据操作类型调用tree的Set或Delete方法，从而还原一个tree）
	// 读取 (8+n) ~ (8+n)+8 位置的 8 个字节，以便确定下一个元素的数据长度，直到读完wal文件

	if size == 0 {
		return tree
	}

	_, err := w.f.Seek(0, 0)
	if err != nil {
		panic(err)
	}

	// 文件指针移动到最后，以便追加
	defer func() {
		_, err := w.f.Seek(size-1, 0)
		if err != nil {
			panic(err)
		}
	}()

	data := make([]byte, size)
	_, err = w.f.Read(data) // 将wal全部读到data内存
	if err != nil {
		panic(err)
	}

	dataLen := int64(0)
	index := int64(0)
	for index < size {
		// todo 抽离为decode函数 ？
		indexData := data[index : index+8]
		buf := bytes.NewBuffer(indexData)
		err := binary.Read(buf, binary.LittleEndian, &dataLen) // 将元素的长度写到dataLen（从将8byte的字节数组转为int64）
		if err != nil {
			panic(err)
		}

		index += 8
		dataArea := data[index : index+dataLen]
		var val kv.Kv
		err = w.marsher.Unmarshal(dataArea, &val)
		if err != nil {
			panic(err)
		}
		if val.Deleted {
			tree.Delete(val.Key)
		} else {
			tree.Set(val.Key, val.Value)
		}

		index += dataLen
	}
	return tree
}

// todo 什么时候reset wal文件？
