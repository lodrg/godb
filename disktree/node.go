package disktree

// Node 接口定义所有节点必须实现的方法
type DiskNode interface {
	Insert(key uint32, value []byte) *DiskInsertResult
	Search(key uint32) (interface{}, bool)
	SearchAll(key uint32) ([][]byte, bool)
	GetKeys() []uint32
	GetPageNumber() uint32
	WriteDisk(logSequenceNumber int32) error
	Delete(key uint32) error
}

type DiskInsertResult struct {
	Key      uint32
	DiskNode DiskNode
}
