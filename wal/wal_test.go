package wal

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"lsmtree/kv"
)

func TestWal(t *testing.T) {
	dir := fmt.Sprintf("out/wal/%v", time.Now().Unix())
	wal := New()
	tree := wal.initMemtable(dir)
	//t.Logf("%#v", tree)

	err := wal.Write(kv.Kv{"1", []byte("1"), false})
	assert.Nil(t, err)

	err = wal.Write(kv.Kv{"2", []byte("2"), false})
	assert.Nil(t, err)

	err = wal.Write(kv.Kv{"2", nil, true})
	assert.Nil(t, err)

	wal = New()
	tree = wal.initMemtable(dir)
	//t.Logf("%#v", tree)
	assert.Equal(t, []kv.Kv{{"1", []byte("1"), false}, {"2", nil, true}}, tree.GetValues())

	// 构造多个wal，验证多个wal的恢复情况
	wal = wal.Reset()
	err = wal.Write(kv.Kv{"1", []byte("1"), false})
	assert.Nil(t, err)

	wal = New()
	mem, imm := wal.Restore(dir)
	assert.Equal(t, dir+"/2.wal.log", mem.GetName())
	assert.Equal(t, []kv.Kv{{"1", []byte("1"), false}}, mem.GetValues())

	assert.Equal(t, 1, len(imm))
	assert.Equal(t, dir+"/1.wal.log", imm[0].GetName())
	assert.Equal(t, []kv.Kv{{"1", []byte("1"), false}, {"2", nil, true}}, imm[0].GetValues())

	err = wal.Delete(imm[0].GetName())
	assert.Nil(t, err)

}
