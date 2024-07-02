package kv

import (
	"encoding/json"
)

type MarshalOp interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

type Json struct {
}

func (j Json) Marshal(v any) ([]byte, error) {
	return json.Marshal(v)
}

func (j Json) Unmarshal(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
