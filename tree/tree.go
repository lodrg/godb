package tree

import "fmt"

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
}
