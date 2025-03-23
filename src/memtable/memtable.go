package memtable

import (
	"golsm/src/skiplist"
	"golsm/src/wal"
)

// MemTable 结构
type MemTable struct {
	skipList *skiplist.SkipList
	log      *wal.WAL
}

// 创建新的MemTable
func New(walPath string, syncWrites bool) (*MemTable, error) {
	// 打开WAL
	log, err := wal.Open(walPath, syncWrites)
	if err != nil {
		return nil, err
	}

	// 创建SkipList
	list := skiplist.NewSkipList(nil)

	// 从WAL恢复数据
	iter, err := log.NewIterator()
	if err != nil {
		log.Close()
		return nil, err
	}
	defer iter.Close()

	// 迭代WAL中的所有记录并重建MemTable
	for {
		record, err := iter.Next()
		if err != nil {
			break
		}

		switch record.Type {
		case wal.TypePut:
			list.Insert(record.Key, record.Value)
		case wal.TypeDelete:
			list.Delete(record.Key)
		}
	}

	return &MemTable{
		skipList: list,
		log:      log,
	}, nil
}

// 关闭MemTable
func (m *MemTable) Close() error {
	return m.log.Close()
}

// 插入键值对
func (m *MemTable) Put(key, value []byte) error {
	// 先写WAL
	err := m.log.Write(wal.Record{
		Type:  wal.TypePut,
		Key:   key,
		Value: value,
	})
	if err != nil {
		return err
	}

	// 再更新SkipList
	m.skipList.Insert(key, value)
	return nil
}

// 删除键
func (m *MemTable) Delete(key []byte) error {
	// 先写WAL
	err := m.log.Write(wal.Record{
		Type:  wal.TypeDelete,
		Key:   key,
		Value: nil,
	})
	if err != nil {
		return err
	}

	// 再更新SkipList
	m.skipList.Delete(key)
	return nil
}
