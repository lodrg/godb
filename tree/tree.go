package tree

import "fmt"
import "strings"

type TreeNode struct {
    Value int
    Children []*TreeNode
}

func NewTreeNode(value int) *TreeNode {
    return &TreeNode{value,[]*TreeNode{}}
}

func (n *TreeNode) AddChild(child *TreeNode) {
    n.Children = append(n.Children, child)
}

func (n *TreeNode) PrintTree(level int){
    fmt.Printf("%s%d\n", strings.Repeat(" ", level*2), n.Value)
    for _, child := range n.Children {
        child.PrintTree(level + 1) 
    }
}
