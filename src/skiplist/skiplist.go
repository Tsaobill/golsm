package skiplist

import (
	"math/rand"
	"time"
)

const (
	maxLevel    = 32
	probability = 0.25
)

type Comparator interface {
	Compare(a, b interface{}) int // 返回负数表示a<b, 0表示a=b，正数代表a>b
}

// Node 跳表节点
type Node struct {
	key     interface{}
	value   interface{}
	forward []*Node // 每层的前向指针
}

type SkipList struct {
	head       *Node
	comparator Comparator
	level      int
	size       int
	r          *rand.Rand
}

func NewSkipList(cmp Comparator) *SkipList {
	if cmp == nil {
		panic("Comparator can not be nil")
	}

	head := &Node{
		forward: make([]*Node, maxLevel),
	}
	return &SkipList{
		head:       head,
		comparator: cmp,
		level:      1,
		r:          rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (sl *SkipList) randomLevel() int {
	level := 1
	for level < maxLevel && sl.r.Float64() < probability {
		level++
	}
	return level
}

func (sl *SkipList) Find(key interface{}) (interface{}, bool) {
	x := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && sl.comparator.Compare(x.forward[i].key, key) < 0 {
			x = x.forward[i]
		}
	}

	x = x.forward[0]
	if x != nil && sl.comparator.Compare(x.key, key) == 0 {
		return x.value, true
	}
	return nil, false
}

func (sl *SkipList) Insert(key, value interface{}) {
	update := make([]*Node, maxLevel)
	x := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && sl.comparator.Compare(x.forward[i].key, key) < 0 {
			x = x.forward[i]
		}
		update[i] = x
	}

	// exist
	x = x.forward[0]
	if x != nil && sl.comparator.Compare(x.key, key) == 0 {
		x.value = value
		return
	}

	level := sl.randomLevel()

	if level > sl.level {
		for i := sl.level; i < level; i++ {
			update[i] = sl.head
		}
		sl.level = level
	}

	newNode := &Node{
		key:     key,
		value:   value,
		forward: make([]*Node, level),
	}

	// put new node to every level
	for i := 0; i < level; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}
	sl.size++
}

// 删除键对应的节点
func (sl *SkipList) Delete(key interface{}) bool {
	update := make([]*Node, maxLevel)
	x := sl.head

	// 查找要删除节点的前向节点
	for i := sl.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && sl.comparator.Compare(x.forward[i].key, key) < 0 {
			x = x.forward[i]
		}
		update[i] = x
	}

	x = x.forward[0]

	// 没找到要删除的节点
	if x == nil || sl.comparator.Compare(x.key, key) != 0 {
		return false
	}

	// 删除节点
	for i := 0; i < sl.level; i++ {
		if update[i].forward[i] != x {
			break
		}
		update[i].forward[i] = x.forward[i]
	}

	// 更新最大层级，如果没有节点在更高的层级上
	for sl.level > 1 && sl.head.forward[sl.level-1] == nil {
		sl.level--
	}

	sl.size--
	return true
}

// 获取跳表大小
func (sl *SkipList) Size() int {
	return sl.size
}

// 迭代器相关功能，用于范围遍历
type Iterator struct {
	list    *SkipList
	current *Node
}

func (sl *SkipList) NewIterator() *Iterator {
	return &Iterator{
		list:    sl,
		current: sl.head.forward[0],
	}
}

func (iter *Iterator) Valid() bool {
	return iter.current != nil
}

func (iter *Iterator) Key() interface{} {
	if !iter.Valid() {
		panic("Invalid iterator")
	}
	return iter.current.key
}

func (iter *Iterator) Value() interface{} {
	if !iter.Valid() {
		panic("Invalid iterator")
	}
	return iter.current.value
}

func (iter *Iterator) Next() {
	if !iter.Valid() {
		panic("Invalid iterator")
	}
	iter.current = iter.current.forward[0]
}

func (iter *Iterator) Seek(key interface{}) {
	x := iter.list.head

	for i := iter.list.level - 1; i >= 0; i-- {
		for x.forward[i] != nil && iter.list.comparator.Compare(x.forward[i].key, key) < 0 {
			x = x.forward[i]
		}
	}

	iter.current = x.forward[0]
}
