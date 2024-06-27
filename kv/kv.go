package kv

// 用于判断查找到此 Key 后，此 Key 是否有效。
type SearchResult int

const (
	None    SearchResult = iota //没有查找到
	Deleted                     //已经被删除
	Success                     // 查找成功
)

type Kv struct {
	Key     string
	Value   []byte // 序列化后存入 使用 MarshalOp
	Deleted bool
}
