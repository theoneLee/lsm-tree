package memtable

import (
	"log"
	"sync"

	"lsmtree/kv"
)

// Tree 二叉树，作为memtable
type Tree struct {
	root  *treeNode
	Count int
	lock  *sync.RWMutex
	name  string //可以用于memtable和immemtable查找key时的顺序
}

type treeNode struct {
	Val   kv.Kv
	Left  *treeNode
	Right *treeNode
}

func NewTree(name string) *Tree {
	return &Tree{
		name:  name,
		root:  nil,
		Count: 0,
		lock:  &sync.RWMutex{},
	}
}

// Search 查找 Key 的值
func (tree *Tree) Search(key string) (kv.Kv, kv.SearchResult) {
	tree.lock.RLock()
	defer tree.lock.RUnlock()

	if tree == nil {
		log.Fatal("tree is nil.")
	}

	// 二分查找
	node := tree.root
	for node != nil {
		if key == node.Val.Key {
			if node.Val.Deleted {
				return kv.Kv{}, kv.Deleted
			}
			return node.Val, kv.Success
		}
		if key < node.Val.Key {
			node = node.Left
		} else {
			node = node.Right
		}
	}
	return kv.Kv{}, kv.None
}

// Set 设置 Key 的值并返回旧值
func (tree *Tree) Set(key string, value []byte) (oldValue kv.Kv, hasOld bool) {
	tree.lock.Lock()
	defer tree.lock.Unlock()

	if tree == nil {
		log.Fatal("set:tree is nil.")
	}

	newNode := &treeNode{
		Val: kv.Kv{
			Key:     key,
			Value:   value,
			Deleted: false,
		},
	}

	current := tree.root
	if current == nil {
		tree.root = newNode
		tree.Count++
		return kv.Kv{}, false
	}

	// 二分查找，找到合适的位置后插入/覆盖/标记删除
	for current != nil {
		if key == current.Val.Key {
			// 覆盖
			old := current.Val
			current.Val.Value = value
			current.Val.Deleted = false
			if old.Deleted {
				return kv.Kv{}, false
			} else {
				return old, true
			}
		}
		if key < current.Val.Key {
			// 找到合适的插入位置
			if current.Left == nil {
				current.Left = newNode
				tree.Count++
				return kv.Kv{}, false
			}
			// 没找到，继续找
			current = current.Left
		} else {
			if current.Right == nil {
				current.Right = newNode
				tree.Count++
				return kv.Kv{}, false
			}
			current = current.Right
		}
	}
	log.Fatal("set:tree set fail") // 不会进入这个地方
	return kv.Kv{}, false
}

// Delete 删除 key 并返回旧值
func (tree *Tree) Delete(key string) (oldValue kv.Kv, hasOld bool) {
	tree.lock.Lock()
	defer tree.lock.Unlock()

	if tree == nil {
		log.Fatal("delete:tree is nil")
	}

	newNode := &treeNode{
		Val: kv.Kv{
			Key:     key,
			Value:   nil,
			Deleted: true,
		},
	}

	if tree.root == nil {
		tree.root = newNode
		return kv.Kv{}, false
	}

	// 二分查找，找到元素进行删除
	current := tree.root
	for current != nil {
		if key == current.Val.Key {
			// 标记删除
			if current.Val.Deleted {
				return kv.Kv{}, false
			}
			current.Val.Deleted = true
			current.Val.Value = nil
			tree.Count--
			return current.Val, true
		}

		if key < current.Val.Key {
			if current.Left == nil {
				current.Left = newNode
				return kv.Kv{}, false
			}
			current = current.Left
		} else {
			if current.Right == nil {
				current.Right = newNode
				return kv.Kv{}, false
			}
			current = current.Right
		}
	}
	return kv.Kv{}, false
}

// GetValues 获取树中的所有元素，这是一个有序元素列表
func (tree *Tree) GetValues() []kv.Kv {
	tree.lock.RLock()
	defer tree.lock.RUnlock()
	// 前序遍历
	var list []kv.Kv
	dfs(tree.root, &list)
	return list
}

func dfs(root *treeNode, list *[]kv.Kv) {
	if root == nil {
		return
	}
	*list = append(*list, root.Val)
	dfs(root.Left, list)
	dfs(root.Right, list)
	return
}

// todo 单测。验证这些实现
