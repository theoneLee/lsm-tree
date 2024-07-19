package db

import (
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"lsmtree/kv"
)

func TestDb_Op(t *testing.T) {
	dir := fmt.Sprintf("out/db/%v", time.Now().Unix())
	err := os.MkdirAll(dir, 0755)
	if err != nil {
		panic(err)
	}

	db := &Db{}
	db = db.Init(dir)

	kv1 := kv.Kv{"1", []byte("1"), false}
	err = db.SetKv(kv1)
	assert.Nil(t, err)
	k, res := db.GetKv("1")
	assert.Equal(t, kv1, k) // 预期是从mem获取
	err = db.SetKv(kv.Kv{"2", []byte("1"), false})
	assert.Nil(t, err)
	err = db.DeleteKv("2")
	assert.Nil(t, err)
	_, res = db.GetKv("2")
	assert.Equal(t, kv.Deleted, res) // 预期是从mem获取

	db.Shutdown()

	t.Log("case:模拟重启，此时wal构造出来memtable,还是可以让db正常工作")
	db = db.Init(dir)
	k, res = db.GetKv("1")
	assert.Equal(t, kv1, k) // 预期是从mem获取

	t.Log("case: set足够多的数据，mem->imm。再imm从wal恢复后，可正常工作。")
	for i := 0; i < 60; i++ {
		err = db.SetKv(kv.Kv{strconv.Itoa(i), []byte("1"), false})
		assert.Nil(t, err)
	}
	k, res = db.GetKv("1")
	assert.Equal(t, kv1, k) // 预期是从imm获取
	//db.Shutdown()
	db.stopCh <- struct{}{} //这里不可以使用shutdown，会触发d.demonTask()
	db = db.Init(dir)
	k, res = db.GetKv("1")
	assert.Equal(t, kv1, k) // 预期是从imm获取

	t.Log("case: 确保imm->sst。从sst恢复后，可正常工作。")
	time.Sleep(11 * time.Second) // 需要确保demonTask触发
	db.stopCh <- struct{}{}
	db = db.Init(dir)
	k, res = db.GetKv("1")
	assert.Equal(t, kv1, k) // 预期是从sst获取

	// todo 构造10个sst。触发合并后再恢复，可正常工作。

}
