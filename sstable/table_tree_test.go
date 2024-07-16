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

func Test_getSstPathList2(t *testing.T) {
	dir := fmt.Sprintf("out/sst/path2/%v", time.Now().Unix())
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		panic(err)
	}

	fileList := []string{
		"1.2.db",
		"3.1.db",
		"1.1.db",
		"3.4.db",
		"2.1.db",
	}
	// 构造数据
	for _, name := range fileList {
		os.Create(path.Join(dir, name))
	}

	expectFileList := []string{
		path.Join(dir, "1.1.db"),
		path.Join(dir, "1.2.db"),
		path.Join(dir, "2.1.db"),
		path.Join(dir, "3.1.db"),
		path.Join(dir, "3.4.db"),
	}

	res := getSstPathList2(dir)
	assert.Equal(t, expectFileList, res)

}

func Test_getSstPathList(t *testing.T) {
	dir := fmt.Sprintf("out/sst/path/%v", time.Now().Unix())
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		panic(err)
	}

	fileList := []string{
		"1.2.db",
		"3.1.db",
		"1.1.db",
		"3.4.db",
		"2.1.db",
	}
	// 构造数据
	for _, name := range fileList {
		os.Create(path.Join(dir, name))
	}

	expectFileList := []string{
		path.Join(dir, "1.1.db"),
		path.Join(dir, "1.2.db"),
		path.Join(dir, "2.1.db"),
		path.Join(dir, "3.1.db"),
		path.Join(dir, "3.4.db"),
	}

	res := getSstPathList(dir)
	assert.Equal(t, expectFileList, res)

}

func TestTableTree_Insert_Search(t1 *testing.T) {
	dir := fmt.Sprintf("out/sst/op_search/%v", time.Now().Unix())
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		panic(err)
	}
	tt := RestoreTableTree(dir)
	tableTree := tt.(*TableTree)
	assert.Equal(t1, 0, len(tableTree.levels))

	imm := memtable.NewTree("")
	imm.Set("1", []byte("1"))
	imm.Set("2", []byte("1"))
	imm.Set("3", []byte("1"))
	imm.Delete("3")

	err = tableTree.Insert(imm)
	assert.Nil(t1, err)

	assert.Equal(t1, 1, len(tableTree.levels))
	_, res := tableTree.Search("3")
	assert.Equal(t1, kv.Deleted, res)

	val, res := tableTree.Search("2")
	assert.Equal(t1, kv.Kv{"2", []byte("1"), false}, val)
	assert.Equal(t1, kv.Success, res)

	_, res = tableTree.Search("6")
	assert.Equal(t1, kv.None, res)
}

func TestTableTree_CompactLevel(t *testing.T) {
	dir := fmt.Sprintf("out/sst/op/%v", time.Now().Unix())
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		panic(err)
	}
	tt := RestoreTableTree(dir)
	tableTree := tt.(*TableTree)
	assert.Equal(t, 0, len(tableTree.levels))

	// 构造多个sst
	imm := memtable.NewTree("")
	imm.Set("1", []byte("1"))
	imm.Set("2", []byte("1"))
	imm.Set("3", []byte("1"))
	err = tableTree.Insert(imm)

	imm = memtable.NewTree("")
	imm.Set("4", []byte("1"))
	err = tableTree.Insert(imm)

	imm = memtable.NewTree("")
	imm.Set("5", []byte("1"))
	imm.Delete("1")
	err = tableTree.Insert(imm)

	assert.Equal(t, 0, len(tableTree.CheckCompactLevels()))

	for i := 0; i < 8; i++ {
		imm = memtable.NewTree("")
		imm.Delete("1")
		err = tableTree.Insert(imm)
	}
	assert.Equal(t, 11, len(tableTree.levels[0].table))
	assert.Equal(t, []int{0}, tableTree.CheckCompactLevels())

	err = tableTree.CompactLevel(0)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(tableTree.levels))
	assert.Equal(t, 0, len(tableTree.levels[0].table))
	assert.Equal(t, 1, len(tableTree.levels[1].table))

	_, res := tableTree.Search("1")
	assert.Equal(t, kv.Deleted, res)

	val, res := tableTree.Search("2")
	assert.Equal(t, kv.Kv{"2", []byte("1"), false}, val)
	assert.Equal(t, kv.Success, res)

	val, res = tableTree.Search("5")
	assert.Equal(t, kv.Kv{"5", []byte("1"), false}, val)
	assert.Equal(t, kv.Success, res)

	_, res = tableTree.Search("16")
	assert.Equal(t, kv.None, res)

	// 重建1.0.db
	tt = RestoreTableTree(dir)
	val, res = tt.Search("2")
	assert.Equal(t, kv.Kv{"2", []byte("1"), false}, val)
	assert.Equal(t, kv.Success, res)

	val, res = tt.Search("5")
	assert.Equal(t, kv.Kv{"5", []byte("1"), false}, val)
	assert.Equal(t, kv.Success, res)

	_, res = tt.Search("16")
	assert.Equal(t, kv.None, res)

	tableTree = tt.(*TableTree)
	assert.Equal(t, 2, len(tableTree.levels))
	assert.Equal(t, 0, len(tableTree.levels[0].table))
	assert.Equal(t, 1, len(tableTree.levels[1].table))
}
