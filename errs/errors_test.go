package errs

import (
	"fmt"
)

func demo() {
	// 每次新增一个模块时，需要在lib库上的moduleMap上新增自己的moduleCode和name。
	/*
		var moduleMap = map[int]string{
			10: "lsmtree",
			11: "xxx",
		}
	*/
	// 后续新增错误可以使用RegisterModuleErr注册进去
	xxxDescription := make(map[ErrCode]Desc) // 按需添加自己需要的错误码
	RegisterModuleErr("xxx", xxxDescription)

	// 后面报错时，如果是有err的错误
	err := fmt.Errorf("1") // 模拟其他引入的err
	NewErr(ErrCodeSstable, err)

	// 如果是不带err的错误
	New(ErrCodeWal)
}
