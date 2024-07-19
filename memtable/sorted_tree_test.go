package memtable

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"lsmtree/kv"
)

func TestTree_Search(t *testing.T) {
	tree := NewTree("1")
	tree.Set("2", []byte("2"))
	tree.Set("1", []byte("1"))
	tree.Delete("2")

	//assert.Equal(t, []kv.Kv{{"1", []byte("1"), false}, {"2", nil, true}}, tree.GetValues())
	data, result := tree.Search("1")
	assert.Equal(t, kv.Kv{"1", []byte("1"), false}, data)
	assert.Equal(t, kv.Success, result)

	data, result = tree.Search("2")
	assert.Equal(t, kv.Kv{}, data)
	assert.Equal(t, kv.Deleted, result)

	data, result = tree.Search("3")
	assert.Equal(t, kv.Kv{}, data)
	assert.Equal(t, kv.None, result)

	tree.Set("3", []byte("3"))
	data, result = tree.Search("3")
	assert.Equal(t, kv.Kv{"3", []byte("3"), false}, data)
	assert.Equal(t, kv.Success, result)

	tree.Set("5", []byte("5"))
	tree.Set("4", []byte("4"))
	tree.Delete("3")
	tree.Delete("5")
	tree.Delete("6") // 删除一个不存在的key，会添加node
	tree.Set("2", []byte("2"))

	data, result = tree.Search("6")
	assert.Equal(t, kv.Kv{}, data)
	assert.Equal(t, kv.Deleted, result)

}

func TestTree_Set(t *testing.T) {
	tree := NewTree("1")
	tree.Set("2", []byte("2"))
	tree.Set("1", []byte("1"))

	assert.Equal(t, []kv.Kv{{"1", []byte("1"), false}, {"2", []byte("2"), false}}, tree.GetValues())

	go func() {
		tree.Set("3", []byte("3"))
		tree.Set("5", []byte("5"))
		tree.Set("4", []byte("4"))
	}()

	go func() {
		tree.GetValues() // go test -race
	}()

	time.Sleep(time.Second)

	assert.Equal(t, []kv.Kv{
		{"1", []byte("1"), false},
		{"2", []byte("2"), false},
		{"3", []byte("3"), false},
		{"4", []byte("4"), false},
		{"5", []byte("5"), false},
	}, tree.GetValues())

}

func TestTree_Delete(t *testing.T) {
	tree := NewTree("1")
	tree.Set("2", []byte("2"))
	tree.Set("1", []byte("1"))
	tree.Delete("2")

	assert.Equal(t, []kv.Kv{{"1", []byte("1"), false}, {"2", nil, true}}, tree.GetValues())

	tree.Set("3", []byte("3"))
	tree.Set("5", []byte("5"))
	tree.Set("4", []byte("4"))
	tree.Delete("3")
	tree.Delete("5")
	tree.Delete("6") // 删除一个不存在的key，会添加node
	tree.Set("2", []byte("2"))

	assert.Equal(t, []kv.Kv{
		{"1", []byte("1"), false},
		{"2", []byte("2"), false},
		{"3", nil, true},
		{"4", []byte("4"), false},
		{"5", nil, true},
		{"6", nil, true},
	}, tree.GetValues())

}

func TestTree_CheckCap(t *testing.T) {
	tree := NewTree("1")
	tree.Delete("91")
	tree.GetName()
	tree.Set("90", []byte("2"))
	assert.Equal(t, false, tree.CheckCap())
	for i := 0; i < 60; i++ {
		tree.Set(strconv.Itoa(i), []byte("2"))
	}
	assert.Equal(t, true, tree.CheckCap())
	for i := 0; i < 60; i++ {
		tree.Set(strconv.Itoa(i), []byte("2"))
	}

	for i := 0; i < 60; i++ {
		tree.Delete(strconv.Itoa(i))
	}

	assert.Equal(t, false, tree.CheckCap())
}

func TestTree_Merge(t *testing.T) {
	tree := NewTree("1")
	tree.Delete("91")

	tree2 := NewTree("2")
	tree2.Set("1", []byte("1"))
	tree2.Set("91", []byte("1"))

	tree.Merge(tree2)
	expect := []kv.Kv{kv.Kv{Key: "1", Value: []byte("1"), Deleted: false}, kv.Kv{Key: "91", Value: []byte("1"), Deleted: false}}
	assert.Equal(t, expect, tree.GetValues())
}
