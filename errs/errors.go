package errs

const (
	// 100001为例 前两位10是模块的标识 后4位0001是该模块内部的详细错误码
	ErrCodeUnknown ErrCode = iota + 100001
	ErrCodeMarshal
	ErrCodeMemtable
	ErrCodeSstable
	ErrCodeWal
)

var lsmTreeDescription = map[ErrCode]Desc{
	ErrCodeUnknown:  {"内部未知错误", "unkonwn error"},
	ErrCodeMarshal:  {"marshal错误", ""},
	ErrCodeMemtable: {"memtable错误", ""},
	ErrCodeSstable:  {"sstable错误", ""},
	ErrCodeWal:      {"wal错误", ""},
}

func init() {
	RegisterModuleErr("lsmtree", lsmTreeDescription)
}
