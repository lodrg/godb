package tree

import (
	"bytes"
	"encoding/binary"
	f "godb/file"
	"log"
)

// Node 接口定义所有节点必须实现的方法
type DiskNode interface {
	Insert(key int, value interface{}) Node
	Search(key int) (interface{}, bool)
	GetKeys() []int
}

func ReadDisk(order int, pager f.DiskPager, pageNumber uint32) Node {
	// 从磁盘读取数据
	data, err := pager.ReadPage(pageNumber)

	buffer := bytes.NewBuffer(data)

	// 读取是否是叶子节点 (1 字节)
	isLeaf := buffer.Next(1)[0] != 0

	// 读取键的数量 (4 字节)
	var keyCount int32
	if err := binary.Read(buffer, binary.LittleEndian, &keyCount); err != nil {
		log.Fatalf("Failed to read keyCount: %v", err)
	}

	// 读取键 (keyCount * 4 字节)
	keys := make([]int, keyCount)
	for i := 0; i < int(keyCount); i++ {
		var key int32
		if err := binary.Read(buffer, binary.LittleEndian, &key); err != nil {
			log.Fatalf("Failed to read key: %v", err)
		}
		keys[i] = int(key)
	}

	if isLeaf {
		// 如果是叶子节点，读取值和 nextPageNumber
		entries := make([]Entry, keyCount)

		for i := 0; i < int(keyCount); i++ {
			// 读取 valueLength (4 字节)
			var valueLength int32
			if err := binary.Read(buffer, binary.LittleEndian, &valueLength); err != nil {
				log.Fatalf("Failed to read valueLength: %v", err)
			}

			// 读取 valueData (变长字段)
			valueData := make([]byte, valueLength)
			if _, err := buffer.Read(valueData); err != nil {
				log.Fatalf("Failed to read valueData: %v", err)
			}

			// 创建键值对
			entries[i] = Entry{
				Key:   keys[i],
				Value: string(valueData),
			}
		}

		// 读取 nextPageNumber (4 字节)
		var nextPageNumber int32
		if err := binary.Read(buffer, binary.LittleEndian, &nextPageNumber); err != nil {
			log.Fatalf("Failed to read nextPageNumber: %v", err)
		}

		// 返回叶子节点
		return &LeafNode{
			baseNode: baseNode{
				keys:  keys,
				order: order,
			},
			nextPageNumber: int(nextPageNumber),
		}
	} else {
		// 如果是中间节点，读取子节点引用
		childrenPageNumbers := make([]int, keyCount+1)

		for i := 0; i < int(keyCount+1); i++ {
			var childPageNumber int32
			if err := binary.Read(buffer, binary.LittleEndian, &childPageNumber); err != nil {
				log.Fatalf("Failed to read childPageNumber: %v", err)
			}
			childrenPageNumbers[i] = int(childPageNumber)
		}

		// 返回中间节点
		return &InternalNode{
			baseNode: baseNode{
				keys:  keys,
				order: order,
			},
			childrenPageNumbers: childrenPageNumbers,
		}
	}
}
