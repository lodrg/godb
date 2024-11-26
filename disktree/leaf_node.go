package tree

import (
	"bytes"
	"encoding/binary"
	f "godb/file"
	"log"
)

// LeafNode 叶子节点
type DiskLeafNode struct {
	Order      int
	PageNumber int
	DiskPager  f.DiskPager
	Entries    []Entry
	next       *DiskLeafNode
}

// NewLeafNode 创建新的叶子节点
func NewLeafNode(order int, pager f.DiskPager, pageNum int) *DiskLeafNode {
	return &DiskLeafNode{
		Entries:    make([]Entry, 0, order-1),
		Order:      order,
		PageNumber: pageNum,
		DiskPager:  pager,
	}
}

// Insert 实现叶子节点的插入
func (n *DiskLeafNode) Insert(key int, value string) DiskNode {
	insertIndex := 0
	for insertIndex < len(n.Entries) && n.Entries[insertIndex].Key < key {
		insertIndex++
	}

	// 如果键已存在，更新值
	if insertIndex < len(n.Entries) && n.Entries[insertIndex].Key == key {
		n.Entries[insertIndex].Value = value
		return n
	}

	// 插入新条目
	n.Entries = append(n.Entries, Entry{})
	copy(n.Entries[insertIndex+1:], n.Entries[insertIndex:])
	n.Entries[insertIndex] = Entry{Key: key, Value: value}
	n.writeDisk()

	// 如果节点需要分裂
	if len(n.Entries) > n.Order {
		return n.split()
	}

	return n
}

// split 分裂叶子节点
func (n *DiskLeafNode) split() DiskNode {
	midIndex := n.Order / 2

	// 创建新的右侧节点
	newNode := NewLeafNode(n.Order, n.DiskPager, n.PageNumber)
	newNode.Entries = append(newNode.Entries, n.Entries[midIndex:]...)
	newNode.writeDisk()

	// 维护叶子节点链表
	n.Entries = n.Entries[:midIndex]
	newNode.next = n.next
	n.next = newNode
	n.writeDisk()

	// 创建父节点
	// parent := NewInternalNode(n.Order)
	// parent.entries = append(parent.entries, Entry{Key: newNode.Entries[0].Key})
	// parent.children = append(parent.children, n, newNode)

	return newNode
}

// Search 在叶子节点中搜索
func (n *DiskLeafNode) Search(key int) (interface{}, bool) {
	for _, entry := range n.Entries {
		if entry.Key == key {
			return entry.Value, true
		}
	}
	return nil, false
}

// GetKeys 获取节点的键列表
func (n *DiskLeafNode) GetKeys() []int {
	keys := make([]int, len(n.Entries))
	for i, entry := range n.Entries {
		keys[i] = entry.Key
	}
	return keys
}
func (n *DiskLeafNode) writeDisk() {
	// 创建一个字节缓冲区
	buffer := bytes.NewBuffer(nil)

	// 写入 isLeaf 标志 (1 byte)
	if err := buffer.WriteByte(1); err != nil {
		log.Fatalf("Failed to write isLeaf flag: %v", err)
	}

	// 写入 keyCount (4 bytes)
	keyCount := int32(len(n.Entries))
	if err := binary.Write(buffer, binary.LittleEndian, keyCount); err != nil {
		log.Fatalf("Failed to write keyCount: %v", err)
	}

	// 写入键值对 (key, valueLength, valueData)
	for _, entry := range n.Entries {
		// 写入 key (4 bytes)
		if err := binary.Write(buffer, binary.LittleEndian, int32(entry.Key)); err != nil {
			log.Fatalf("Failed to write key: %v", err)
		}

		// 写入 valueLength (4 bytes)
		valueLength := int32(len(entry.Value))
		if err := binary.Write(buffer, binary.LittleEndian, valueLength); err != nil {
			log.Fatalf("Failed to write valueLength: %v", err)
		}

		// 写入 valueData (valueLength bytes)
		if _, err := buffer.Write([]byte(entry.Value)); err != nil {
			log.Fatalf("Failed to write valueData: %v", err)
		}
	}

	// 写入 nextPageNumber (4 bytes)
	nextPageNumber := int32(0)
	if n.next != nil {
		nextPageNumber = int32(n.next.PageNumber)
	}
	if err := binary.Write(buffer, binary.LittleEndian, nextPageNumber); err != nil {
		log.Fatalf("Failed to write nextPageNumber: %v", err)
	}

	// 将缓冲区内容写入磁盘
	data := buffer.Bytes()
	n.DiskPager.WritePage(n.PageNumber, data)
}
