package sstable

import (
	"os"
	"sync"
)

// SsTable 存储在磁盘上。 [数据区,稀疏索引区,元数据]
//
//	其中磁盘上的稀疏索引区可以直接反序列化为map[string]Position
//	   数据区写入的时候是一个一个kv.Kv写入的，因此还原时需要通过Position进行切分后再反序列化为kv.Kv
type SsTable struct {
	f        *os.File // 文件句柄，sstable写在这个文件下
	filePath string

	tableMetaInfo MetaInfo // 元数据

	// 确定该 SSTable 中是否存在此 Key // todo 还可以使用布隆过滤器来优化
	startPoints map[string]Position // 文件的稀疏索引

	lock sync.Locker
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
