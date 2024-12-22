package tests

import (
	"godb/disktree"
	"testing"
)

func TestTree(t *testing.T) {
	// 创建一个4阶B+树
	tree := disktree.NewBPTree(4, 10)

	tree.Print()

	tree.Insert(11, "11")
	tree.Insert(22, "22")
	tree.Insert(33, "33")
	tree.Insert(44, "44")
	tree.Insert(55, "55")

	tree.Print()
}
