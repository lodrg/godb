package disktree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"godb/logger"
	"log"
)

// InternalNode 内部节点
type DiskInternalNode struct {
	Order               uint32
	PageNumber          uint32
	Keys                []uint32
	ChildrenPageNumbers []uint32
	DiskPager           *DiskPager
	RedoLog             *RedoLog
}

// NewInternalNode 创建新的内部节点
func NewInternalNode(order uint32, pager *DiskPager, pageNum uint32, redolog *RedoLog) *DiskInternalNode {
	node := &DiskInternalNode{
		Order:               order,
		PageNumber:          pageNum,
		DiskPager:           pager,
		RedoLog:             redolog,
		Keys:                make([]uint32, 0, order-1),
		ChildrenPageNumbers: make([]uint32, 0, order),
	}
	return node
}

// Insert 实现内部节点的插入
func (n *DiskInternalNode) Insert(key uint32, value []byte) *DiskInsertResult {
	// 找到合适的子节点
	insertIndex := 0
	for insertIndex < len(n.Keys) && n.Keys[insertIndex] <= key {
		insertIndex++
	}

	//fmt.Printf("insert key: %d \nindex: %d \nnode: %+v\n", key, insertIndex, n)

	// 递归插入到子节点
	childPage := n.ChildrenPageNumbers[insertIndex]
	child := ReadDisk(n.Order, n.DiskPager, childPage, n.RedoLog)
	result := child.Insert(key, value)

	if result != nil {
		// 子节点分裂，需要插入新的键和子节点指针
		childPage := child.GetPageNumber()
		n.insertIntoNode(result.Key, childPage)

		// 内部节点最多可以有 order-1 个键
		if uint32(len(n.Keys)) <= n.Order-1 {
			return nil // 不需要进一步分裂
		}
		return n.splitInternalNode()
	}

	return nil
}

// insertIntoNode 插入键和子节点到当前节点
func (n *DiskInternalNode) insertIntoNode(key uint32, childPage uint32) {
	insertIndex := 0
	for insertIndex < len(n.Keys) && key >= n.Keys[insertIndex] {
		insertIndex++
	}

	// 插入键
	n.Keys = append(n.Keys[:insertIndex], append([]uint32{key}, n.Keys[insertIndex:]...)...)

	// 修改：正确处理子节点页码
	if len(n.ChildrenPageNumbers) == 0 {
		// 如果是空节点，初始化子节点页码
		n.ChildrenPageNumbers = []uint32{0, childPage}
	} else {
		// 在正确的位置插入子节点页码
		n.ChildrenPageNumbers = append(n.ChildrenPageNumbers[:insertIndex+1],
			append([]uint32{childPage}, n.ChildrenPageNumbers[insertIndex+1:]...)...)
	}

	logSequenceNumber, err := n.RedoLog.LogInsertInternalNormal(int32(n.PageNumber), int32(key), int(childPage))
	if err != nil {
		logger.Error("failed to log internal node")
	}
	if err := n.WriteDisk(logSequenceNumber); err != nil {
		log.Fatal(err)
	}
}

// splitInternalNode 分裂内部节点
func (n *DiskInternalNode) splitInternalNode() *DiskInsertResult {
	// 创建新的右侧节点
	newNodePage, err := n.DiskPager.AllocateNewPage()
	if err != nil {
		log.Fatal("Failed to allocate new page")
	}
	newNode := NewInternalNode(n.Order, n.DiskPager, uint32(newNodePage), n.RedoLog)

	// 计算中间位置
	midIndex := len(n.Keys) / 2
	// 获取要提升的键
	promotedKey := n.Keys[midIndex]

	// 将右半部分的键移动到新节点
	// midIndex + 1 因为中间的键会被提升到父节点
	logger.Debug("init Node.keys : %v", newNode.Keys)
	newNode.Keys = append(newNode.Keys, n.Keys[midIndex+1:]...)
	logger.Debug("newNode.keys : %v", newNode.Keys)
	// 将当前节点保留左半部分
	n.Keys = n.Keys[:midIndex]

	// 移动对应的子节点指针
	// 子节点数量比键多1
	logger.Debug("init Node.childrenPageNumbers : %v", newNode.ChildrenPageNumbers)
	newNode.ChildrenPageNumbers = append(newNode.ChildrenPageNumbers,
		n.ChildrenPageNumbers[midIndex+1:]...)
	logger.Debug("newNode.childrenPageNumbers : %v", newNode.ChildrenPageNumbers)
	n.ChildrenPageNumbers = n.ChildrenPageNumbers[:midIndex+1]

	logSequenceNumber, err := n.RedoLog.LogInsertInternalSplit(int32(n.PageNumber))
	// 写回磁盘
	if err := newNode.WriteDisk(logSequenceNumber); err != nil {
		log.Fatalf("Failed to write new internal node to disk: %v", err)
	}
	if err := n.WriteDisk(logSequenceNumber); err != nil {
		log.Fatalf("Failed to write internal node to disk: %v", err)
	}

	return &DiskInsertResult{
		Key:      promotedKey,
		DiskNode: newNode,
	}
}

// Search 在内部节点中搜索
func (n *DiskInternalNode) Search(key uint32) (interface{}, bool) {
	// 找到合适的子节点
	index := 0
	for index < len(n.Keys) && n.Keys[index] <= key {
		index++
	}

	// 加载对应的子节点
	childPageNumber := n.ChildrenPageNumbers[index]
	child := ReadDisk(n.Order, n.DiskPager, childPageNumber, n.RedoLog)

	return child.Search(key)
}

func (n *DiskInternalNode) SearchAll(key uint32) ([][]byte, bool) {
	// 找到合适的子节点
	index := 0
	for index < len(n.Keys) && n.Keys[index] <= key {
		index++
	}

	result := make([][]byte, 0, n.Order)
	for index < len(n.ChildrenPageNumbers) {
		childPageNumber := n.ChildrenPageNumbers[index]
		child := ReadDisk(n.Order, n.DiskPager, childPageNumber, n.RedoLog)
		all, _ := child.SearchAll(key)
		result = append(result, all...)
		if index < len(n.Keys) && key < n.Keys[index] {
			break
		}
		index++
	}
	return result, true
}

// GetKeys 获取节点的键列表
func (n *DiskInternalNode) GetKeys() []uint32 {
	if len(n.Keys) == 0 {
		return nil
	}
	Keys := make([]uint32, len(n.Keys))
	for i, key := range n.Keys {
		Keys[i] = key
	}
	return Keys
}

func (n *DiskInternalNode) GetPageNumber() uint32 {
	return n.PageNumber
}

// WriteDisk 将叶子节点写入磁盘
// internal node format:
// isLeaf (1 byte)
// keyCount (4 bytes)
// keys (4 * keyCount bytes)
// childrenPageNumbers size (4 bytes)
// childrenPageNumbers (4 * keyCount bytes)

// isLeaf (1 byte) | keyCount (4 bytes) | [key (4 bytes)]*keyCount |childrenPageNumbers size (4 bytes) |childrenPageNumbers (4 * keyCount bytes)
// 1 + 4 + 12 + 4 + 40 = 61
// WriteDisk 将内部节点写入磁盘
func (node *DiskInternalNode) WriteDisk(logSequenceNumber int32) error {
	fmt.Printf("Writing internal node to page %d\n", node.PageNumber) // 添加日志
	//fmt.Printf("keys: %v\n", node.Keys)                               // 添加日志
	//fmt.Printf("Children: %v\n", node.ChildrenPageNumbers) // 添加日志
	// 获取页面大小
	pageSize := node.DiskPager.GetPageSize()

	// 创建一个固定大小的缓冲区
	buffer := bytes.NewBuffer(make([]byte, 0, pageSize))

	// 写入 isLeaf 标志 (0 表示内部节点)
	if err := buffer.WriteByte(0); err != nil {
		return err
	}

	// 写入键的数量 (uint32)
	if err := binary.Write(buffer, binary.BigEndian, uint32(len(node.Keys))); err != nil {
		return err
	}

	// 写入键 (uint32)
	for _, key := range node.Keys {
		if err := binary.Write(buffer, binary.BigEndian, key); err != nil {
			return err
		}
	}

	// 写入子节点页码 (int32)
	for _, childPage := range node.ChildrenPageNumbers {
		if err := binary.Write(buffer, binary.BigEndian, uint32(childPage)); err != nil {
			return err
		}
	}

	// 确保数据长度等于页面大小
	data := buffer.Bytes()
	if len(data) < node.DiskPager.GetPageSize() {
		padding := make([]byte, node.DiskPager.GetPageSize()-len(data))
		data = append(data, padding...)
	}

	return node.DiskPager.WritePage(int(node.PageNumber), data, logSequenceNumber)
}

// Delete 删除指定 key 的数据
func (n *DiskInternalNode) Delete(key uint32) error {
	// 找到应该递归的子节点
	childIndex := 0
	for childIndex < len(n.Keys) && key >= n.Keys[childIndex] {
		childIndex++
	}
	childPage := n.ChildrenPageNumbers[childIndex]
	child := ReadDisk(n.Order, n.DiskPager, childPage, n.RedoLog)
	return child.Delete(key)
}
