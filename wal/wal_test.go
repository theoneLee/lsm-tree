package wal

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"lsmtree/kv"
)

func TestWal_initMemtable(t *testing.T) {
	dir := fmt.Sprintf("./out/wal/%v", time.Now().Unix())
	wal := New()
	tree := wal.initMemtable(dir)
	t.Logf("%#v", tree)

	err := wal.Write(kv.Kv{"1", []byte("1"), false})
	assert.Nil(t, err)

	err = wal.Write(kv.Kv{"2", []byte("2"), false})
	assert.Nil(t, err)

	err = wal.Write(kv.Kv{"2", nil, true})
	assert.Nil(t, err)

	wal = New()
	tree = wal.initMemtable(dir)
	t.Logf("%#v", tree)
	assert.Equal(t, []kv.Kv{{"1", []byte("1"), false}, {"2", nil, true}}, tree.GetValues())

}
