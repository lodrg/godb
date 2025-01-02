package disktree

// Node 接口定义所有节点必须实现的方法
type DiskNode interface {
	Insert(key uint32, value string) *DiskInsertResult
	Search(key uint32) (interface{}, bool)
	SearchAll(key uint32) (interface{}, bool)
	GetKeys() []uint32
	GetPageNumber() uint32
	WriteDisk() error
}

type DiskInsertResult struct {
	Key      uint32
	DiskNode DiskNode
}
