package errs

import (
	"errors"
	"fmt"
)

type BaseError struct {
	ErrStr string
	Code   int
}

func (e *BaseError) Error() string {
	return fmt.Sprintf("errorMsg:%s, code:%d", e.ErrStr, e.Code)
}

func FromError(err error) (code int, has bool) {
	if target := (&BaseError{}); errors.As(err, &target) {
		return target.Code, true
	}

	if err != nil {
		// return unknown code
		return 1, true
	}

	return -1, false
}

const (
	ErrCodeUnknown = iota + 1 // 从1开始
	ErrCodeMarshal
	ErrCodeMemtable
	ErrCodeSstable
	ErrCodeWal
)

var (
	ErrUnknown = BaseError{
		ErrStr: "unknown",
		Code:   ErrCodeUnknown,
	}

	ErrMemtable = BaseError{
		ErrStr: "",
		Code:   ErrCodeMemtable,
	}

	ErrSstable = BaseError{
		ErrStr: "",
		Code:   ErrCodeSstable,
	}
	ErrWal = BaseError{
		ErrStr: "",
		Code:   ErrCodeWal,
	}
)

// 外部只需要使用New和Newf即可

func New(err BaseError) error {
	return fmt.Errorf("%w", err)
}

func Newf(err BaseError, format string, args ...any) error {
	format = format + ",:%w"
	args = append(args, err)
	return fmt.Errorf(format, args)
}
