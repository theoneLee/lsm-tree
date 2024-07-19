package errs

import (
	"errors"
	"fmt"
)

// 新增一个模块时，需要在这里注册
var moduleMap = map[int]string{
	10: "lsmtree",
}

// 错误信息 国际化 Ch/En
var Lang = "Ch"

func NewErr(errCode ErrCode, err error) error {
	module := moduleMap[int(errCode/1000)]

	var cause string
	if Lang == "Ch" {
		cause = errorDescription[errCode].Ch
	} else {
		cause = errorDescription[errCode].En
	}

	return BaseError{
		module: module,
		cause:  cause,
		code:   errCode,
		err:    err,
	}
}

func New(errCode ErrCode) error {
	module := moduleMap[int(errCode/1000)]

	var cause string
	if Lang == "Ch" {
		cause = errorDescription[errCode].Ch
	} else {
		cause = errorDescription[errCode].En
	}

	return BaseError{
		module: module,
		cause:  cause,
		code:   errCode,
	}
}

// === 以下为内部实现

type BaseError struct {
	module string
	cause  string  // 用户能理解的错误信息。包含错误原因，应该怎么解决这个错误
	code   ErrCode // 错误码，用于给开发快速定位错误发生的地点
	err    error   // 原始错误
}

func (e BaseError) Error() string {
	return fmt.Sprintf("module:%v cause:%v code:%v err:%v", e.module, e.cause, e.code, e.err)
}

func FromError(err error) (code ErrCode, has bool) {
	if target := (&BaseError{}); errors.As(err, &target) {
		return target.code, true
	}

	if err != nil {
		// return unknown code
		return 1, true
	}

	return -1, false
}

type ErrCode int

func RegisterModuleErr(moduleName string, customError map[ErrCode]Desc) {
	for code, desc := range customError {
		moduleCode := int(code / 10000)
		if moduleMap[moduleCode] != moduleName {
			panic(fmt.Sprintf("请检查错误模块,code:%v,moduleMap[moduleCode]:%v,moduleName:%v", code, moduleMap[moduleCode], moduleName))
		}
		errorDescription[code] = desc
	}
}

type Desc struct {
	Ch string
	En string
}

var errorDescription = map[ErrCode]Desc{}
