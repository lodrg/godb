package tree

// InternalNode 内部节点
type DiskInternalNode struct {
	Order               int
	PageNumber          int
	Entry               []Entry
	ChildrenPageNumbers []int
}

// NewInternalNode 创建新的内部节点
func NewInternalNode(order int) *InternalNode {
	return &InternalNode{
		baseNode: baseNode{
			entries: make([]Entry, 0, order-1),
			order:   order,
		},
		children: make([]Node, 0, order),
	}
}

// Insert 实现内部节点的插入
func (n *InternalNode) Insert(key int, value interface{}) Node {
	// 找到合适的子节点
	insertIndex := 0
	for insertIndex < len(n.entries) && n.entries[insertIndex].Key <= key {
		insertIndex++
	}

	// 递归插入到子节点
	child := n.children[insertIndex]
	newChild := child.Insert(key, value)

	// 如果子节点没有分裂
	if newChild == child {
		return n
	}

	// 处理子节点分裂的情况
	return n.insertChild(newChild, insertIndex)
}

// insertChild 插入新的子节点
func (n *InternalNode) insertChild(newChild Node, insertIndex int) Node {
	// 获取新节点的第一个键作为分隔键
	var splitKey int
	switch child := newChild.(type) {
	case *InternalNode:
		splitKey = child.entries[0].Key
		n.entries = append(n.entries, Entry{})
		copy(n.entries[insertIndex+1:], n.entries[insertIndex:])
		n.entries[insertIndex] = Entry{Key: splitKey}

		n.children = append(n.children, nil)
		copy(n.children[insertIndex+1:], n.children[insertIndex:])
		n.children[insertIndex] = child.children[0]
		n.children[insertIndex+1] = child.children[1]
	case *LeafNode:
		splitKey = child.entries[0].Key
		n.entries = append(n.entries, Entry{Key: splitKey})
		n.children = append(n.children, newChild)
	}

	// 检查是否需要分裂
	if len(n.entries) >= n.order {
		return n.split()
	}

	return n
}

// split 分裂内部节点
func (n *InternalNode) split() Node {
	midIndex := (n.order - 1) / 2

	// 创建新的右侧节点
	newNode := NewInternalNode(n.order)
	newNode.entries = append(newNode.entries, n.entries[midIndex+1:]...)
	newNode.children = append(newNode.children, n.children[midIndex+1:]...)

	midKey := n.entries[midIndex]

	// 更新当前节点
	n.entries = n.entries[:midIndex]
	n.children = n.children[:midIndex+1]

	// 创建新的父节点
	parent := NewInternalNode(n.order)
	parent.entries = append(parent.entries, midKey)
	parent.children = append(parent.children, n, newNode)

	return parent
}

// Search 在内部节点中搜索
func (n *InternalNode) Search(key int) (interface{}, bool) {
	// 找到合适的子节点
	index := 0
	for index < len(n.entries) && n.entries[index].Key <= key {
		index++
	}
	if index > 0 {
		index--
	}
	return n.children[index].Search(key)
}

// GetKeys 获取节点的键列表
func (n *InternalNode) GetKeys() []int {
	keys := make([]int, len(n.entries))
	for i, entry := range n.entries {
		keys[i] = entry.Key
	}
	return keys
}
