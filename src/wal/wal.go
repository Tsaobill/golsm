package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// 操作类型
const (
	TypePut    byte = 1
	TypeDelete byte = 2
)

// 错误定义
var (
	ErrInvalidChecksum = errors.New("invalid checksum")
	ErrInvalidRecord   = errors.New("invalid record")
)

// WAL 结构体
type WAL struct {
	file    *os.File
	mu      sync.Mutex
	size    int64
	syncOps bool // 是否同步写入磁盘
}

// 记录结构体
type Record struct {
	Type  byte
	Key   []byte
	Value []byte
}

// 打开WAL文件
func Open(path string, syncOps bool) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	// 获取文件大小
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	return &WAL{
		file:    file,
		size:    stat.Size(),
		syncOps: syncOps,
	}, nil
}

// 关闭WAL
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.file.Close()
}

// 编码变长整数
func encodeVarint(x uint64) []byte {
	var buf [10]byte
	n := binary.PutUvarint(buf[:], x)
	return buf[:n]
}

// 写入一条记录
func (w *WAL) Write(record Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 计算记录大小
	keyLen := len(record.Key)
	valueLen := len(record.Value)

	// 创建缓冲区
	keyLenEncoded := encodeVarint(uint64(keyLen))
	valueLenEncoded := encodeVarint(uint64(valueLen))

	recordSize := 1 + len(keyLenEncoded) + len(valueLenEncoded) + keyLen + valueLen + 4
	buf := make([]byte, recordSize)

	// 写入记录类型
	buf[0] = record.Type

	// 写入键长度
	copy(buf[1:], keyLenEncoded)
	offset := 1 + len(keyLenEncoded)

	// 写入值长度
	copy(buf[offset:], valueLenEncoded)
	offset += len(valueLenEncoded)

	// 写入键
	copy(buf[offset:], record.Key)
	offset += keyLen

	// 写入值
	copy(buf[offset:], record.Value)
	offset += valueLen

	// 计算校验和并写入
	checksum := crc32.ChecksumIEEE(buf[:offset])
	binary.LittleEndian.PutUint32(buf[offset:], checksum)

	// 写入文件
	_, err := w.file.Write(buf)
	if err != nil {
		return err
	}

	// 如果需要同步写入磁盘
	if w.syncOps {
		if err := w.file.Sync(); err != nil {
			return err
		}
	}

	// 更新文件大小
	w.size += int64(recordSize)
	return nil
}

// 批量写入记录
func (w *WAL) WriteBatch(records []Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 计算总大小并分配缓冲区
	totalSize := 0
	for _, record := range records {
		keyLenEncoded := encodeVarint(uint64(len(record.Key)))
		valueLenEncoded := encodeVarint(uint64(len(record.Value)))
		totalSize += 1 + len(keyLenEncoded) + len(valueLenEncoded) + len(record.Key) + len(record.Value) + 4
	}

	buf := make([]byte, totalSize)
	offset := 0

	// 写入所有记录
	for _, record := range records {
		keyLen := len(record.Key)
		valueLen := len(record.Value)
		keyLenEncoded := encodeVarint(uint64(keyLen))
		valueLenEncoded := encodeVarint(uint64(valueLen))

		// 写入记录类型
		buf[offset] = record.Type
		offset++

		// 写入键长度
		copy(buf[offset:], keyLenEncoded)
		offset += len(keyLenEncoded)

		// 写入值长度
		copy(buf[offset:], valueLenEncoded)
		offset += len(valueLenEncoded)

		// 写入键
		copy(buf[offset:], record.Key)
		offset += keyLen

		// 写入值
		copy(buf[offset:], record.Value)
		offset += valueLen

		// 计算校验和
		recordSize := 1 + len(keyLenEncoded) + len(valueLenEncoded) + keyLen + valueLen
		checksum := crc32.ChecksumIEEE(buf[offset-recordSize : offset])
		binary.LittleEndian.PutUint32(buf[offset:offset+4], checksum)
		offset += 4
	}

	// 写入文件
	_, err := w.file.Write(buf)
	if err != nil {
		return err
	}

	// 如果需要同步写入磁盘
	if w.syncOps {
		if err := w.file.Sync(); err != nil {
			return err
		}
	}

	// 更新文件大小
	w.size += int64(totalSize)
	return nil
}

// 从WAL重建MemTable的迭代器
type Iterator struct {
	file    *os.File
	offset  int64
	fileEnd int64
}

// 创建迭代器
func (w *WAL) NewIterator() (*Iterator, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// 复制文件句柄以便并行读取
	f, err := os.Open(w.file.Name())
	if err != nil {
		return nil, err
	}

	return &Iterator{
		file:    f,
		offset:  0,
		fileEnd: w.size,
	}, nil
}

// 读取变长整数
func readUvarint(r io.Reader) (uint64, int, error) {
	var x uint64
	var s uint
	var b byte
	var err error

	buf := make([]byte, 1)
	for i := 0; ; i++ {
		_, err = r.Read(buf)
		if err != nil {
			return 0, 0, err
		}
		b = buf[0]

		if b < 0x80 {
			if i > 9 || i == 9 && b > 1 {
				return 0, 0, errors.New("binary: varint overflows 64 bits")
			}
			return x | uint64(b)<<s, i + 1, nil
		}
		x |= uint64(b&0x7f) << s
		s += 7
	}
}

// 迭代获取下一条记录
func (it *Iterator) Next() (*Record, error) {
	// 检查是否到文件末尾
	if it.offset >= it.fileEnd {
		return nil, io.EOF
	}

	// 定位到偏移位置
	_, err := it.file.Seek(it.offset, 0)
	if err != nil {
		return nil, err
	}

	// 读取记录类型
	typeBuf := make([]byte, 1)
	_, err = it.file.Read(typeBuf)
	if err != nil {
		return nil, err
	}

	recordType := typeBuf[0]
	if recordType != TypePut && recordType != TypeDelete {
		return nil, ErrInvalidRecord
	}

	// 读取键长度
	keyLen, keyLenSize, err := readUvarint(it.file)
	if err != nil {
		return nil, err
	}

	// 读取值长度
	valueLen, valueLenSize, err := readUvarint(it.file)
	if err != nil {
		return nil, err
	}

	// 读取键
	key := make([]byte, keyLen)
	_, err = io.ReadFull(it.file, key)
	if err != nil {
		return nil, err
	}

	// 读取值
	value := make([]byte, valueLen)
	_, err = io.ReadFull(it.file, value)
	if err != nil {
		return nil, err
	}

	// 读取校验和
	checksumBuf := make([]byte, 4)
	_, err = io.ReadFull(it.file, checksumBuf)
	if err != nil {
		return nil, err
	}

	// 计算记录大小
	recordSize := 1 + keyLenSize + valueLenSize + int(keyLen) + int(valueLen) + 4

	// 验证校验和
	// 需要重新计算前面的数据的校验和
	_, err = it.file.Seek(it.offset, 0)
	if err != nil {
		return nil, err
	}

	data := make([]byte, recordSize-4)
	_, err = io.ReadFull(it.file, data)
	if err != nil {
		return nil, err
	}

	checksum := crc32.ChecksumIEEE(data)
	readChecksum := binary.LittleEndian.Uint32(checksumBuf)

	if checksum != readChecksum {
		return nil, ErrInvalidChecksum
	}

	// 更新偏移量
	it.offset += int64(recordSize)

	return &Record{
		Type:  recordType,
		Key:   key,
		Value: value,
	}, nil
}

// 关闭迭代器
func (it *Iterator) Close() error {
	return it.file.Close()
}

// 截断WAL
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Truncate(0); err != nil {
		return err
	}

	if _, err := w.file.Seek(0, 0); err != nil {
		return err
	}

	w.size = 0
	return nil
}
