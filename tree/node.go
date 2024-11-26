package tree

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
