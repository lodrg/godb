package tests

import (
	"godb/disktree"
	f "godb/file"
	"testing"
)

func TestTree(t *testing.T) {
	//listenConn()
	diskPager, _ := f.NewDiskPager("disktest.db", 80)
	// 创建一个4阶B+树
	tree := disktree.NewBPTree(4, 10, *diskPager)

	tree.Print()

	tree.Insert(11, "11")
	tree.Insert(22, "22")
	tree.Insert(33, "33")
	tree.Insert(44, "44")
	tree.Insert(55, "55")

	tree.Print()
}
