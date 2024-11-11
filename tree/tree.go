package tree

import "fmt"

// Entry 存储键值对
type Entry struct {
	Key   int
	Value interface{}
}

// Node 接口定义所有节点必须实现的方法
type Node interface {
	Insert(key int, value interface{}) Node
	Search(key int) (interface{}, bool)
	GetKeys() []int
}

// baseNode 提供基础字段和方法
type baseNode struct {
	entries []Entry
	order   int
}

// 内部节点
type InternalNode struct {
	baseNode
	children []Node
}

// 叶子节点
type LeafNode struct {
	baseNode
	next *LeafNode
}

// B+ 树结构
type BPTree struct {
	root  Node
	order int
}

// NewBPTree 创建新的 B+ 树
func NewBPTree(order int) *BPTree {
	if order < 3 {
		order = 3
	}
	return &BPTree{
		root:  nil,
		order: order,
	}
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

// Insert 插入键值对
func (t *BPTree) Insert(key int, value interface{}) {
	if t.root == nil {
		t.root = NewLeafNode(t.order)
	}
	t.root = t.root.Insert(key, value)
}

// Search 查找键对应的值
func (t *BPTree) Search(key int) (interface{}, bool) {
	if t.root == nil {
		return nil, false
	}
	return t.root.Search(key)
}

// Print 打印树结构
func (t *BPTree) Print() {
	fmt.Println("=== B+ Tree ===")
	t.printNode(t.root, "")
}

// printNode 递归打印节点
func (t *BPTree) printNode(node Node, prefix string) {
	switch n := node.(type) {
	case *InternalNode:
		fmt.Printf("%sInternal%v\n", prefix, n.GetKeys())
		for _, child := range n.children {
			t.printNode(child, prefix+"  ")
		}
	case *LeafNode:
		fmt.Printf("%sLeaf%v", prefix, n.GetKeys())
		if n.next != nil {
			fmt.Print("→")
		}
		fmt.Println()
	}
} // Insert 实现叶子节点的插入
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
	fmt.Println("中间节点", midIndex)

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

// Insert 实现内部节点的插入
func (n *InternalNode) Insert(key int, value interface{}) Node {
	// 找到合适的子节点
	insertIndex := 0
	for insertIndex < len(n.entries) && n.entries[insertIndex].Key <= key {
		insertIndex++
	}
	// if insertIndex > 0 {
	// 	insertIndex--
	// }

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
