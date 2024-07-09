package sstable

import (
	"encoding/binary"
	"fmt"
	"os"
	"sync"

	"lsmtree/errs"
	"lsmtree/kv"
	"lsmtree/memtable"
)

type SstOp interface {
	Encode(imm memtable.ImmemtableOp) error
	Search(key string) (kv.Kv, kv.SearchResult)
	Decode() (memtable.MemtableOp, error)
}

// 元数据 描述了稀疏索引和数据区的位置。用于在字节数组上切分（编解码）
type MetaInfo struct {
	version int64
	// 数据区
	dataStart int64
	dataLen   int64

	// 稀疏索引区
	pointStart int64
	pointLen   int64
}

// Position 元素定位，存储在稀疏索引区中，表示一个元素的起始位置和长度
type Position struct {
	Start   int64
	Len     int64
	Deleted bool // Key 已经被删除
}

// SsTable 存储在磁盘上。 [数据区,稀疏索引区,元数据]
//
//	其中磁盘上的稀疏索引区可以直接反序列化为map[string]Position
//	   数据区写入的时候是一个一个kv.Kv写入的，因此还原时需要通过Position进行切分后再反序列化为kv.Kv
type SsTable struct {
	f        *os.File // 文件句柄，sstable写在这个文件下
	filePath string

	tableMetaInfo MetaInfo // 元数据

	// 确定该 SSTable 中是否存在此 Key // todo 还可以使用布隆过滤器来优化，这样在startPoints不需要一直放到内存，有需要再取
	startPoints map[string]Position // 文件的稀疏索引

	lock    sync.Locker
	marsher kv.MarshalOp
}

func (s *SsTable) Decode() (memtable.MemtableOp, error) {
	s.lock.Lock()
	defer s.lock.Unlock()

	// 将sst转化为memtable
	var tree memtable.MemtableOp
	s.restoreStartPoints()

	for key, pos := range s.startPoints {
		item, res := s.getKv(pos)
		if res == kv.Deleted {
			tree.Delete(key)
			continue
		}
		tree.Set(key, item.Value)
	}
	return tree, nil
}

func (s *SsTable) Search(key string) (kv.Kv, kv.SearchResult) {
	s.lock.Lock()
	defer s.lock.Unlock()

	if len(s.startPoints) == 0 {
		// 尝试读取f，然后构造s.startPoint
		s.restoreStartPoints()
	}

	// 从startPoint拿到key是否存在，然后直接从f读取
	if pos, ok := s.startPoints[key]; ok {
		return s.getKv(pos)
	}
	return kv.Kv{}, kv.None
}

func (s *SsTable) getKv(pos Position) (kv.Kv, kv.SearchResult) {
	if pos.Deleted {
		return kv.Kv{}, kv.Deleted
	}
	_, err := s.f.Seek(pos.Start, 0)
	if err != nil {
		panic(err)
	}
	size := pos.Len
	data := make([]byte, size)
	_, err = s.f.Read(data) // 将key对应的字节数据全部读到data内存
	if err != nil {
		panic(err)
	}
	item := kv.Kv{}
	err = s.marsher.Unmarshal(data, &item)
	if err != nil {
		panic(err)
	}
	return item, kv.Success
}

func (s *SsTable) Encode(imm memtable.ImmemtableOp) error {
	s.lock.Lock()
	defer s.lock.Unlock()

	// 将imm的每一个kv拿出来序列化，然后写入startPoints，写入f。
	//   直到imm的kv都写完（即数据区写完）
	start := int64(0)
	sp := make(map[string]Position)
	for _, item := range imm.GetValues() {
		itemByte, err := s.marsher.Marshal(item)
		if err != nil {
			return errs.Newf(errs.ErrSstable, "marshal err:%v", err)
		}
		itemByteLen := len(itemByte)
		sp[item.Key] = Position{
			Start:   start,
			Len:     int64(itemByteLen),
			Deleted: item.Deleted,
		}

		err = binary.Write(s.f, binary.LittleEndian, itemByte)
		if err != nil {
			return errs.Newf(errs.ErrSstable, "Write err:%v", err)
		}

		start = start + int64(itemByteLen)
	}

	//   再序列化startPoints，写入索引区
	spBytes, err := s.marsher.Marshal(sp)
	if err != nil {
		return errs.Newf(errs.ErrSstable, "Marshal err:%v", err)
	}
	spStart := start
	spBytesLen := int64(len(spBytes))
	err = binary.Write(s.f, binary.LittleEndian, spBytes)
	if err != nil {
		return errs.Newf(errs.ErrSstable, "Write err:%v", err)
	}
	start = start + spBytesLen

	//   再写入元数据
	info := MetaInfo{
		version:    1,
		dataStart:  0,
		dataLen:    spStart - 1, // todo 这里需要验证下准确性
		pointStart: spStart,
		pointLen:   spBytesLen,
	}
	fmt.Printf("info:%#v", info)
	infoByte, err := s.marsher.Marshal(info)
	if err != nil {
		return errs.Newf(errs.ErrSstable, "Marshal err:%v", err)
	}
	err = binary.Write(s.f, binary.LittleEndian, infoByte)
	if err != nil {
		return errs.Newf(errs.ErrSstable, "Write err:%v", err)
	}

	return nil
}

func (s *SsTable) restoreStartPoints() {
	info := s.restoreMetaInfo()

	// 从f 读取StartPoints
	_, err := s.f.Seek(info.pointStart, 0)
	if err != nil {
		panic(err)
	}
	data := make([]byte, info.pointLen)
	_, err = s.f.Read(data) // 将StartPoints 对应的字节数据全部读到data内存
	if err != nil {
		panic(err)
	}
	sp := make(map[string]Position)
	err = s.marsher.Unmarshal(data, &sp)
	if err != nil {
		panic(err)
	}
	s.startPoints = sp
}

func (s *SsTable) restoreMetaInfo() MetaInfo {
	stat, err := s.f.Stat()
	if err != nil {
		panic(err)
	}
	size := int64(40)
	fileSize := stat.Size()
	metaStart := fileSize - size // 取最后40个byte，即MetaInfo的长度（5个int64）
	_, err = s.f.Seek(metaStart, 0)
	data := make([]byte, size)
	_, err = s.f.Read(data) // 将MetaInfo对应的字节数据全部读到data内存
	if err != nil {
		panic(err)
	}
	info := MetaInfo{}
	err = s.marsher.Unmarshal(data, &info)
	if err != nil {
		panic(err)
	}
	return info
}

func NewSst(path string) SstOp {
	// todo 区分读写
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	return &SsTable{
		f:             f,
		filePath:      path,
		tableMetaInfo: MetaInfo{},
		startPoints:   nil,
		lock:          &sync.Mutex{},
		marsher:       kv.Json{},
	}
}
