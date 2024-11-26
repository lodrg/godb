package tree

// LeafNode 叶子节点
type LeafNode struct {
	baseNode
	next *LeafNode
}

// NewLeafNode 创建新的叶子节点
func NewLeafNode(order int) *LeafNode {
	return &LeafNode{
		baseNode: baseNode{
			entries: make([]Entry, 0, order-1),
			order:   order,
		},
		next: nil,
	}
}

// Insert 实现叶子节点的插入
func (n *LeafNode) Insert(key int, value interface{}) Node {
	insertIndex := 0
	for insertIndex < len(n.entries) && n.entries[insertIndex].Key < key {
		insertIndex++
	}

	// 如果键已存在，更新值
	if insertIndex < len(n.entries) && n.entries[insertIndex].Key == key {
		n.entries[insertIndex].Value = value
		return n
	}

	// 插入新条目
	n.entries = append(n.entries, Entry{})
	copy(n.entries[insertIndex+1:], n.entries[insertIndex:])
	n.entries[insertIndex] = Entry{Key: key, Value: value}

	// 如果节点需要分裂
	if len(n.entries) > n.order {
		return n.split()
	}

	return n
}

// split 分裂叶子节点
func (n *LeafNode) split() Node {
	midIndex := n.order / 2

	// 创建新的右侧节点
	newNode := NewLeafNode(n.order)
	newNode.entries = append(newNode.entries, n.entries[midIndex:]...)
	n.entries = n.entries[:midIndex]

	// 维护叶子节点链表
	newNode.next = n.next
	n.next = newNode

	// 创建父节点
	parent := NewInternalNode(n.order)
	parent.entries = append(parent.entries, Entry{Key: newNode.entries[0].Key})
	parent.children = append(parent.children, n, newNode)

	return parent
}

// Search 在叶子节点中搜索
func (n *LeafNode) Search(key int) (interface{}, bool) {
	for _, entry := range n.entries {
		if entry.Key == key {
			return entry.Value, true
		}
	}
	return nil, false
}

// GetKeys 获取节点的键列表
func (n *LeafNode) GetKeys() []int {
	keys := make([]int, len(n.entries))
	for i, entry := range n.entries {
		keys[i] = entry.Key
	}
	return keys
}
