package disktree

import (
	f "godb/file"
	"log"
	"testing"
)

func TestTree(t *testing.T) {

	dbfileName := "test_disk.db"
	diskPager, err := f.NewDiskPager(dbfileName, 80, 80)

	if err != nil {
		log.Fatal("Failed to allocate new page")
	}
	// 创建一个4阶B+树
	tree := NewBPTree(4, 10, diskPager)

	tree.Print()

	tree.Insert(11, []byte("11"))
	tree.Insert(22, []byte("22"))
	tree.Insert(33, []byte("33"))
	tree.Insert(44, []byte("44"))
	tree.Insert(55, []byte("55"))

	tree.Search(33)

	tree.Print()
}
