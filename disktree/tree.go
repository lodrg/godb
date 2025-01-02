package disktree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	f "godb/file"
	"log"
	"strings"
)

// B+ 树结构
type BPTree struct {
	rootPageNumber uint32
	order          uint32
	DiskPager      f.DiskPager
	ValueLength    uint32
}

// NewBPTree 创建新的 B+ 树
func NewBPTree(order uint32, valueLength uint32, diskPager *f.DiskPager) *BPTree {

	//diskPager, err := f.NewDiskPager(dbfileName, 80, 80)

	//if err != nil {
	//	log.Fatal("Failed to allocate new page")
	//}

	if order < 3 {
		order = 3
	}
	if diskPager.GetTotalPage() == 0 {
		// 0
		diskPager.AllocateNewPage()
		//fmt.Println("diskPager.AllocateNewPage", 0)
		// 1
		rootPageNum, err := diskPager.AllocateNewPage()
		//fmt.Println("root rootPageNum", rootPageNum)
		if err != nil {
			log.Fatal("Failed to allocate new page")
		}
		//fmt.Println("value length:", valueLength)
		root := NewLeafNode(order, valueLength, diskPager, uint32(rootPageNum))
		if err := root.WriteDisk(); err != nil {
			log.Fatal(err)
		}
		bp := &BPTree{
			rootPageNumber: uint32(rootPageNum),
			order:          order,
			DiskPager:      *diskPager,
			ValueLength:    valueLength,
		}
		bp.writeMetadata()
		return bp
	}
	// 检查读取到的数据是否足够
	// 从数据的前 4 字节读取 rootPageNumber
	rootPageNumber := readMetadata(*diskPager)

	return &BPTree{
		rootPageNumber: uint32(rootPageNumber),
		order:          order,
		DiskPager:      *diskPager,
	}
}

func readMetadata(diskPager f.DiskPager) int {
	data, err := diskPager.ReadPage(0)
	if err != nil {
		log.Fatalf("Failed to read metadata: %v", err)
	}

	if len(data) < 4 {
		log.Fatalf("Metadata page size is too small: expected at least 4 bytes, got %d bytes", len(data))
	}

	rootPageNumber := int(binary.BigEndian.Uint32(data[:4]))
	fmt.Println("read form metadata rootPageNumber:", rootPageNumber)
	return rootPageNumber
}

func (bp *BPTree) writeMetadata() {
	// 创建一个与页大小相同的缓冲区
	buffer := make([]byte, bp.DiskPager.GetPageSize())

	// 使用 binary.Write 将 rootPageNumber 写入缓冲区
	rootPageNumber := uint32(bp.rootPageNumber) // 假设 rootPageNumber 是 int 类型
	binary.BigEndian.PutUint32(buffer[0:4], uint32(rootPageNumber))

	// 将缓冲区写入 pager 的第 0 页
	if err := bp.DiskPager.WritePage(0, buffer); err != nil {
		log.Fatalf("Failed to write metadata: %v", err)
	}
}

// Insert 插入键值对
func (t *BPTree) Insert(key uint32, value string) {
	fmt.Printf("Attempting to insert key: %d, value: %s\n", key, value)
	root := ReadDisk(t.order, &t.DiskPager, t.rootPageNumber)

	result := root.Insert(key, value)
	if result != nil {
		fmt.Printf("Split occurred, creating new root\n")
		rootPageNum, err := t.DiskPager.AllocateNewPage()
		_, _ = t.DiskPager.AllocateNewPage()
		if err != nil {
			log.Fatal("Failed to allocate new page")
		}
		newRoot := NewInternalNode(t.order, &t.DiskPager, uint32(rootPageNum))

		// 修改：先设置子节点页码，再设置键
		oldRootPage := root.GetPageNumber()
		newChildPage := result.DiskNode.GetPageNumber()

		// 正确设置子节点页码和键
		newRoot.ChildrenPageNumbers = []uint32{oldRootPage, newChildPage}
		newRoot.Keys = []uint32{result.Key}

		t.rootPageNumber = newRoot.PageNumber

		// 确保正确的写入顺序
		if err := root.WriteDisk(); err != nil {
			log.Fatalf("Failed to write old root: %v", err)
		}
		if err := result.DiskNode.WriteDisk(); err != nil {
			log.Fatalf("Failed to write new child: %v", err)
		}
		if err := newRoot.WriteDisk(); err != nil {
			log.Fatalf("Failed to write new root: %v", err)
		}
		t.writeMetadata()
	}
}

// ReadDisk 从磁盘中读取节点并返回 DiskNode（InternalNode 或 LeafNode）

func ReadDisk(order uint32, pager *f.DiskPager, pageNumber uint32) DiskNode {
	// 从 pager 读取指定页的数据
	data, err := pager.ReadPage(int(pageNumber))
	if err != nil {
		log.Fatalf("Failed to read page %d: %v", pageNumber, err)
	}
	//fmt.Println("Binary representation:")
	//for _, b := range data {
	//	fmt.Printf("%08b ", b) // Print each byte as an 8-bit binary number
	//}
	fmt.Println()
	//fmt.Printf("Reading page %d from disk\n", pageNumber) // 添加日志
	//fmt.Printf("Raw data length: %d\n", len(data))        // 添加日志
	// 创建一个 bytes.Buffer 来解析数据
	buffer := bytes.NewReader(data)

	// 读取 isLeaf (1 byte)
	isLeafByte := make([]byte, 1)
	if _, err := buffer.Read(isLeafByte); err != nil {
		log.Fatalf("Failed to read isLeaf byte: %v", err)
	}
	isLeaf := isLeafByte[0] != 0

	// 读取 keyCount (4 bytes)
	var keyCount uint32
	if err := binary.Read(buffer, binary.BigEndian, &keyCount); err != nil {
		log.Fatalf("Failed to read keyCount: %v", err)
	}

	// 读取 Keys (keyCount 个键值对)
	keys := make([]uint32, keyCount)
	for i := uint32(0); i < keyCount; i++ {
		// 读取 Key (4 bytes)
		var key uint32
		if err := binary.Read(buffer, binary.BigEndian, &key); err != nil {
			log.Fatalf("Failed to read key: %v", err)
		}
		keys[i] = key
	}

	if isLeaf {

		//fmt.Println("this is leaf >>")

		// 读取 valueLength (4 bytes)
		var valueLength uint32
		if err := binary.Read(buffer, binary.BigEndian, &valueLength); err != nil {
			log.Fatalf("Failed to read keyCount: %v", err)
		}
		//fmt.Println("read of valueLength:", valueLength)

		// 读取 Keys (keyCount 个键值对)
		values := make([][]byte, keyCount)
		for i := uint32(0); i < keyCount; i++ {
			// 创建指定长度的切片来存储 value
			value := make([]byte, valueLength)
			// 直接读取指定长度的字节
			if _, err := buffer.Read(value); err != nil {
				log.Fatalf("Failed to read value: %v", err)
			}
			values[i] = value
		}
		//fmt.Println("values : ", values)

		// 解析 LeafNode 的特有字段
		var nextPageNumber uint32
		if err := binary.Read(buffer, binary.BigEndian, &nextPageNumber); err != nil {
			log.Fatalf("Failed to read NextPageNumber: %v", err)
		}

		// 创建并返回 LeafNode
		node := &DiskLeafNode{
			Order:       order,
			PageNumber:  pageNumber,
			Keys:        keys,
			Values:      values,
			ValueLength: valueLength,
			DiskPager:   pager,
		}
		return node
	} else {

		// 解析 InternalNode 的特有字段
		childrenPageNumbers := make([]uint32, keyCount+1)
		for i := uint32(0); i < keyCount+1; i++ {
			var childPageNumber uint32
			if err := binary.Read(buffer, binary.BigEndian, &childPageNumber); err != nil {
				log.Fatalf("Failed to read child page number: %v", err)
			}
			childrenPageNumbers[i] = childPageNumber
			//fmt.Println("childPagenum:", childPageNumber)
		}

		// 创建并返回 InternalNode
		node := &DiskInternalNode{
			Order:               order,
			PageNumber:          pageNumber,
			Keys:                keys,
			ChildrenPageNumbers: childrenPageNumbers,
			DiskPager:           pager,
		}
		//fmt.Printf("Reading InternalNode: %+v\n", node)
		return node
	}
}

// Search 查找键对应的值
func (t *BPTree) Search(key uint32) (interface{}, bool) {
	root := ReadDisk(t.order, &t.DiskPager, t.rootPageNumber)
	//readDisk first
	if root == nil {
		return nil, false
	}
	return root.Search(key)
}

func (t *BPTree) SearchAll(key uint32) (interface{}, bool) {
	root := ReadDisk(t.order, &t.DiskPager, t.rootPageNumber)
	if root == nil {
		return nil, false
	}
	return root.SearchAll(key)
}

// Print 打印树结构
// Print 打印整棵 B+ 树
func (t *BPTree) Print() {
	fmt.Println("\n========== B+ Tree Print Information ==========")
	fmt.Printf("Root Page Number: %d\n", t.rootPageNumber)
	fmt.Printf("Tree Order: %d\n", t.order)
	fmt.Printf("Total Pages: %d\n", t.DiskPager.GetTotalPage())
	fmt.Println("---------------------------------------------")

	root := ReadDisk(t.order, &t.DiskPager, t.rootPageNumber)
	if root == nil {
		fmt.Println("Empty Tree")
		return
	}

	t.printNodeDetailed(root, 0)
	fmt.Println("\n============== End of Tree ==================")
}

// printNodeDetailed 递归打印节点的详细信息
func (t *BPTree) printNodeDetailed(node DiskNode, depth int) {
	indent := strings.Repeat("    ", depth)
	prefix := strings.Repeat("│   ", depth)

	switch n := node.(type) {
	case *DiskInternalNode:
		fmt.Printf("%s┌── Internal Node (Page: %d)\n", prefix, n.PageNumber)
		fmt.Printf("%s│   ├── Keys: %v\n", prefix, n.GetKeys())
		fmt.Printf("%s│   ├── Children Pages: %v\n", prefix, n.ChildrenPageNumbers)
		fmt.Printf("%s│   ├── key Count: %d\n", prefix, len(n.Keys))
		fmt.Printf("%s│   └── Children Count: %d\n", prefix, len(n.ChildrenPageNumbers))

		//打印每个子节点
		for i, childPage := range n.ChildrenPageNumbers {
			fmt.Printf("%s├── Child %d:", indent, i)
			child := ReadDisk(t.order, &t.DiskPager, childPage)
			t.printNodeDetailed(child, depth+1)
		}

	case *DiskLeafNode:
		fmt.Printf("%s┌── Leaf Node (Page: %d)\n", prefix, n.PageNumber)
		fmt.Printf("%s│   ├── key Count: %d\n", prefix, len(n.Keys))
		//fmt.Printf("%s│   ├── Next Leaf: %d\n", prefix)
		fmt.Printf("%s│   ├── Keys:\n", prefix)

		//打印键值对
		for i, key := range n.Keys {
			fmt.Printf("%s│       [%d] Key: %d, Value: %s\n",
				prefix, i, key, n.Values[i])
		}
	}
}
