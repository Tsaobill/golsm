package main

import (
	"fmt"
	"golsm/src/skiplist"
)

func main() {
	// 创建一个使用整数键的跳表
	list := skiplist.NewSkipList(skiplist.IntComparator{})

	// 插入一些数据
	list.Insert(3, "value3")
	list.Insert(1, "value1")
	list.Insert(7, "value7")
	list.Insert(5, "value5")

	// 查找
	key := 7
	if val, found := list.Find(key); found {
		fmt.Printf("Found key %d, value: %v\n", key, val)
	}
	fmt.Printf("=====================================\n")

	// 通过迭代器遍历
	iter := list.NewIterator()
	fmt.Printf("Iterator\n")

	for iter.Valid() {
		fmt.Printf("Key: %v, Value: %v\n", iter.Key(), iter.Value())
		iter.Next()
	}
	fmt.Printf("=====================================\n")

	// 删除
	list.Delete(key)

	// 查找确认已删除
	if _, found := list.Find(key); !found {
		fmt.Printf("Key %d has been deleted\n", key)
	}
}
