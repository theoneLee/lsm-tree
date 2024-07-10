package sstable

import (
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"lsmtree/kv"
	"lsmtree/memtable"
)

func TestSst(t *testing.T) {
	dir := fmt.Sprintf("out/sst/%v", time.Now().Unix())
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		panic(err)
	}
	name := fmt.Sprintf("%v.%v%v", 0, 1, sstFileSuffix)
	sst := NewSst(path.Join(dir, name))
	imm := memtable.NewTree("")
	imm.Set("1", []byte("1"))
	imm.Set("2", []byte("1"))
	imm.Set("3", []byte("1"))
	imm.Delete("3")
	err = sst.Encode(imm)
	assert.Nil(t, err)
	sstInst := sst.(*SsTable)
	t.Logf("restoreMeta:%#v", sstInst.restoreMetaInfo())

	// 验证 索引区，数据区，是否都符合预期。
	mem, err := sstInst.Decode()
	assert.Nil(t, err)
	assert.Equal(t, imm.GetValues(), mem.GetValues())
	t.Logf("startPoints:%#v", sstInst.startPoints)

	_, res := sstInst.Search("3")
	assert.Equal(t, kv.Deleted, res)

	k, res := sst.Search("2")
	assert.Equal(t, kv.Success, res)
	assert.Equal(t, kv.Kv{"2", []byte("1"), false}, k)

	k, res = sst.Search("6")
	assert.Equal(t, kv.None, res)

}
