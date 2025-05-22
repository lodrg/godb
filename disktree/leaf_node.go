package disktree

import (
	"bytes"
	"encoding/binary"
	"godb/logger"
	"log"
)

// LeafNode 叶子节点
type DiskLeafNode struct {
	Order       uint32
	PageNumber  uint32
	DiskPager   *DiskPager
	RedoLog     *RedoLog
	Keys        []uint32
	Values      [][]byte
	ValueLength uint32
}

// NewLeafNode 创建新的叶子节点
func NewLeafNode(order uint32, valueLength uint32, pager *DiskPager, pageNum uint32, redolog *RedoLog) *DiskLeafNode {
	return &DiskLeafNode{
		Keys:        make([]uint32, 0, order),
		Values:      make([][]byte, 0, order),
		ValueLength: valueLength,
		Order:       order,
		PageNumber:  pageNum,
		DiskPager:   pager,
		RedoLog:     redolog,
	}
}

// Insert 实现叶子节点的插入
func (n *DiskLeafNode) Insert(key uint32, value []byte) *DiskInsertResult {
	insertIndex := 0
	for insertIndex < len(n.Keys) && n.Keys[insertIndex] < key {
		insertIndex++
	}

	// 如果键已存在，更新值
	if insertIndex < len(n.Keys) && n.Keys[insertIndex] == key {
		n.Values[insertIndex] = value
		return &DiskInsertResult{
			Key:      n.Keys[0], // 分裂键为新节点的第一个键
			DiskNode: n,
		}
	}

	// 插入新条目
	n.Keys = append(n.Keys, 0)
	copy(n.Keys[insertIndex+1:], n.Keys[insertIndex:])
	n.Keys[insertIndex] = key

	n.Values = append(n.Values, nil)
	copy(n.Values[insertIndex+1:], n.Values[insertIndex:])
	n.Values[insertIndex] = value
	logger.Debug("values : %x \n", n.Values)
	logger.Debug("values : %x \n", n.Values)

	logSequenceNumber, err := n.RedoLog.LogInsertLeafNormal(int32(n.PageNumber), int32(key), value)
	if err != nil {
		logger.Error("failed to insert leaf node log")
	}
	if err := n.WriteDisk(logSequenceNumber); err != nil {
		log.Fatalf("Failed to write leaf node: %v", err)
	}

	// 如果节点需要分裂
	if uint32(len(n.Keys)) > n.Order {
		return n.split()
	}

	return nil
}

// split 分裂叶子节点
func (n *DiskLeafNode) split() *DiskInsertResult {
	midIndex := n.Order / 2

	// 创建新的右侧节点
	newNodePage, err := n.DiskPager.AllocateNewPage()
	_, _ = n.DiskPager.AllocateNewPage()
	logger.Debug("newNodePage: %v", newNodePage)

	if err != nil {
		log.Fatal("Failed to allocate new page")
	}
	logger.Debug("when split the valueLength is %d", n.ValueLength)
	newNode := NewLeafNode(n.Order, n.ValueLength, n.DiskPager, uint32(newNodePage), n.RedoLog)
	newNode.Keys = append(newNode.Keys, n.Keys[midIndex:]...)
	newNode.Values = append(newNode.Values, n.Values[midIndex:]...)

	if err := newNode.WriteDisk(-1); err != nil {
		//log.Fatalf("Failed to write new node: %v", err)
		logger.Error("Failed to write new node %v", err)
	}

	// 维护叶子节点链表
	n.Keys = n.Keys[:midIndex]
	n.Values = n.Values[:midIndex]
	//logger.Debug("values :", n.Values)

	logSequenceNumber, err := n.RedoLog.LogInsertLeafSplit(int32(n.PageNumber))
	if err != nil {
		log.Fatal("Failed to insert leaf node log")
	}
	if err := n.WriteDisk(logSequenceNumber); err != nil {
		log.Fatalf("Failed to write node: %v", err)
	}
	logger.Debug("return key is %v", newNode.Keys[0])

	return &DiskInsertResult{
		Key:      newNode.Keys[0],
		DiskNode: newNode,
	}
}

// Search 在叶子节点中搜索
func (n *DiskLeafNode) Search(key uint32) (interface{}, bool) {
	// 使用二分查找提高搜索效率
	left, right := 0, len(n.Keys)-1
	for left <= right {
		mid := left + (right-left)/2
		if n.Keys[mid] == key {
			logger.Debug("key is %v", n.Keys[mid])
			logger.Debug("value is %v", n.Values[mid])
			return n.Values[mid], true // 返回对应的值
		} else if n.Keys[mid] < key {
			left = mid + 1
		} else {
			right = mid - 1
		}
	}
	return nil, false // 未找到时返回
}

func (n *DiskLeafNode) SearchAll(key uint32) ([][]byte, bool) {
	result := make([][]byte, 0)

	for i := 0; i < len(n.Keys); i++ {
		if key == n.Keys[i] {
			result = append(result, n.Values[i])
		} else if key < n.Keys[i] {
			// 由于keys是有序的，当找到更大的key时可以停止搜索
			break
		}
	}
	return result, true
}

// GetKeys 获取节点的键列表
func (n *DiskLeafNode) GetKeys() []uint32 {
	keys := make([]uint32, len(n.Keys))
	for i, key := range n.Keys {
		keys[i] = key
	}
	return keys
}

// WriteDisk 将叶子节点写入磁盘
// isLeaf (1 byte) | keyCount (4 bytes) | [key (4 bytes)]*keyCount | valueLength (4 bytes) | valueData (valueLength bytes)] * keyCount | nextPageNumber (4 bytes)
// 1 + 4 + 16 + 4 + 40 + 4 = 69
func (n *DiskLeafNode) WriteDisk(logSequenceNumber int32) error {
	//fmt.Printf("Writing leaf node to page %d\n", n.PageNumber)
	//fmt.Printf("Keys: %v\n", n.Keys)

	// 创建一个字节缓冲区
	//buffer := bytes.NewBuffer(nil)
	//buffer := make([]byte, 1024)
	buffer := new(bytes.Buffer)

	// 写入 isLeaf 标志 (1 byte)
	if err := buffer.WriteByte(1); err != nil {
		log.Fatalf("Failed to write isLeaf flag: %v", err)
	}

	// 写入 keyCount (4 bytes)
	keyCount := uint32(len(n.Keys))
	//logger.Debug("keyCount:", keyCount)
	if err := binary.Write(buffer, binary.BigEndian, keyCount); err != nil {
		//log.Fatalf("Failed to write keyCount: %v", err)
		logger.Error("Failed to write keyCount %v", err)
	}

	// 写入键 (key)
	for _, key := range n.Keys {
		// 写入 key (4 bytes)
		if err := binary.Write(buffer, binary.BigEndian, key); err != nil {
			log.Fatalf("Failed to write key: %v", err)
		}
	}
	//logger.Debug("valueLength: ", n.ValueLength)

	// 写入 valueLength (4 bytes)
	valueLength := n.ValueLength
	if err := binary.Write(buffer, binary.BigEndian, valueLength); err != nil {
		//log.Fatalf("Failed to write ValueLength: %v", err)
		logger.Error("Failed to write ValueLength: %v", err)
	}

	// 写入值 (value)
	//logger.Debug("value length:", len(n.Values))
	logger.Debug("value length: %v", valueLength)
	for _, value := range n.Values {
		logger.Debug("value: %v", string(value))
		if n.ValueLength < uint32(len(value)) {
			log.Fatalf("value length larger than fixed length: valueLength: %d value: %d", n.ValueLength, len(value))
		} else if n.ValueLength >= uint32(len(value)) {
			// 将值写入缓冲区
			if _, err := buffer.Write(value); err != nil {
				log.Fatalf("Failed to write value: %v", err)
			}
			// 用 0 填充剩余部分以使页面大小固定
			paddingLength := n.ValueLength - uint32(len(value))
			padding := make([]byte, paddingLength) // 创建填充字节切片
			if _, err := buffer.Write(padding); err != nil {
				log.Fatalf("Failed to write padding: %v", err)
			}
		}
	}

	// 将缓冲区内容写入磁盘
	logger.Debug("buffer: %v \n", buffer.Bytes())
	//logger.Debug("buffer:", string(buffer.Bytes()))
	logger.Debug("buffer: %x \n", buffer.Bytes())
	data := buffer.Bytes()
	if len(data) < n.DiskPager.GetPageSize() {
		padding := make([]byte, n.DiskPager.GetPageSize()-len(data))
		data = append(data, padding...)
	}
	n.DiskPager.WritePage(int(n.PageNumber), data, logSequenceNumber)
	return nil
}

func (n *DiskLeafNode) GetPageNumber() uint32 {
	return n.PageNumber
}

// Delete 删除指定 key 的数据
func (n *DiskLeafNode) Delete(key uint32) error {
	for i, k := range n.Keys {
		if k == key {
			// 删除 key 和 value
			n.Keys = append(n.Keys[:i], n.Keys[i+1:]...)
			n.Values = append(n.Values[:i], n.Values[i+1:]...)
			logSequenceNumber, err := n.RedoLog.LogInsertLeafNormal(int32(n.PageNumber), int32(key), nil)
			if err != nil {
				return err
			}
			if err := n.WriteDisk(logSequenceNumber); err != nil {
				return err
			}
			return nil
		}
	}
	return nil // 没找到也算成功
}
