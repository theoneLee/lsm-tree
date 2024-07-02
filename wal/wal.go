package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
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
	f    *os.File // memtable的wal
	path string   // memtable的wal
	dir  string
	lock *sync.Mutex

	marsher kv.MarshalOp
}

func New() *Wal {
	w := &Wal{}
	w.lock = &sync.Mutex{}
	w.marsher = kv.Json{}
	return w
}

// Write 将kv写入wal
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

const walFileSuffix = ".wal.log" // wal文件最大的序号为memtable的wal，其余的为

// 从wal文件恢复memtable。
func (w *Wal) initMemtable(dir string) memtable.MemtableOp {
	start := time.Now()
	defer func() {
		log.Println("Load wal cost:", time.Since(start))
	}()
	// 获取这个目录下最大序号的文件
	walFileName := getMemtableFileName(dir)

	walPath := path.Join(dir, walFileName)
	f, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	w.dir = dir
	w.f = f
	w.path = walPath
	return w.loadToMemory()
}

func getMemtableFileName(dir string) string {
	files, err := os.ReadDir(dir)
	if err != nil {
		panic(err)
	}
	// 获取文件名最大的项目
	if len(files) == 0 {
		return "1" + walFileSuffix
	}
	memtableFile := files[0]
	for i := 1; i < len(files); i++ {
		if memtableFile.Name() < files[i].Name() {
			memtableFile = files[i]
		}
	}
	return memtableFile.Name()
}

func getImmemtableFileNames(dir string) []string {
	memtableFileName := getMemtableFileName(dir)
	memtableIndex := strings.ReplaceAll(memtableFileName, walFileSuffix, "")
	_memtableIndex, err := strconv.Atoi(memtableIndex)
	if err != nil {
		panic(err)
	}
	immemtableFileNames := []string{}
	// 检查是否存在_memtableIndex-1的文件
	for i := _memtableIndex - 1; i > 0; i-- {
		filename := fmt.Sprintf("%v%v", i, walFileSuffix)
		_, err := os.Stat(path.Join(dir, filename))
		if err != nil {
			break
		}
		immemtableFileNames = append(immemtableFileNames, filename)
	}
	return immemtableFileNames
}

// 从wal文件上还原为一个memtable
func (w *Wal) loadToMemory() memtable.MemtableOp {
	w.lock.Lock()
	defer w.lock.Unlock()

	path := w.path
	f := w.f
	marsher := w.marsher

	return w.decode(path, f, marsher)
}

// 将wal文件decode为memtable或者immemtable
func (w *Wal) decode(path string, f *os.File, marsher kv.MarshalOp) *memtable.Tree {
	info, _ := os.Stat(path)
	size := info.Size()
	tree := memtable.NewTree(path)
	//首先读取文件开头的 8 个字节，确定第一个元素的字节数量 n，然后将 8 ~ (8+n) 范围中的二进制数据反序列化为treeNode
	//（根据操作类型调用tree的Set或Delete方法，从而还原一个tree）
	// 读取 (8+n) ~ (8+n)+8 位置的 8 个字节，以便确定下一个元素的数据长度，直到读完wal文件

	if size == 0 {
		return tree
	}

	_, err := f.Seek(0, 0)
	if err != nil {
		panic(err)
	}

	// 文件指针移动到最后，以便追加
	defer func() {
		_, err := f.Seek(size-1, 0)
		if err != nil {
			panic(err)
		}
	}()

	data := make([]byte, size)
	_, err = f.Read(data) // 将wal全部读到data内存
	if err != nil {
		panic(err)
	}

	dataLen := int64(0)
	index := int64(0)
	for index < size {
		indexData := data[index : index+8]
		buf := bytes.NewBuffer(indexData)
		err := binary.Read(buf, binary.LittleEndian, &dataLen) // 将元素的长度写到dataLen（从将8byte的字节数组转为int64）
		if err != nil {
			panic(err)
		}

		index += 8
		dataArea := data[index : index+dataLen]
		var val kv.Kv
		err = marsher.Unmarshal(dataArea, &val)
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

func (w *Wal) Restore(dir string) (memtable.MemtableOp, []memtable.ImmemtableOp) {
	var memt memtable.MemtableOp
	var immemList []memtable.ImmemtableOp
	memt = w.initMemtable(dir)
	immemList = w.initImmemtable(dir)
	return memt, immemList
}

func (w *Wal) initImmemtable(dir string) []memtable.ImmemtableOp {
	var list []memtable.ImmemtableOp
	files := getImmemtableFileNames(dir) // imm的文件名是从大到小的顺序的。即后续imm列表的key的内容是从新到旧的。
	for _, file := range files {
		walPath := path.Join(dir, file)
		f, err := os.OpenFile(walPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			panic(err)
		}
		tree := w.decode(walPath, f, w.marsher)
		list = append(list, tree)
	}
	return list
}

// Delete 删除wal文件
func (w *Wal) Delete(filePath string) error {
	w.lock.Lock()
	defer w.lock.Unlock()
	return os.Remove(filePath)
}

// Reset 创建一个新的wal供memtable使用
func (w *Wal) Reset() *Wal {
	w.lock.Lock()
	defer w.lock.Unlock()

	memtableFileName := getMemtableFileName(w.dir)
	memtableIndex := strings.ReplaceAll(memtableFileName, walFileSuffix, "")
	_memtableIndex, err := strconv.Atoi(memtableIndex)
	if err != nil {
		panic(err)
	}
	newIndex := _memtableIndex + 1
	filename := fmt.Sprintf("%v%v", newIndex, walFileSuffix) //创建一个序号更大的wal文件
	w.path = path.Join(w.dir, filename)

	f, err := os.OpenFile(w.path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	w.f = f
	return w
}

//todo 单测
