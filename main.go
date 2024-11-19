package main

import (
	"bufio"
	"fmt"
	f "godb/file"
	"godb/tree"
	"log"
	"net"
	"strconv"
)

const PORT = 8088

func main() {
	//listenConn()

	// 创建一个4阶B+树
	tree := tree.NewBPTree(4)

	// 插入测试数据
	testData := map[int]string{
		1:  "一",
		4:  "四",
		7:  "七",
		10: "十",
		2:  "二",
		5:  "五",
		8:  "八",
		3:  "三",
		6:  "六",
		9:  "九",
		11: "十一",
		12: "十二",
		13: "十三",
		14: "十四",
	}

	// 插入数据并打印树的状态
	for k, v := range testData {
		tree.Insert(k, v)
		fmt.Printf("\n插入 %d:%s 后的树结构:\n", k, v)
		tree.Print()
	}

	// 搜索测试
	fmt.Println("\n搜索测试:")
	for k := 1; k <= 10; k++ {
		if v, found := tree.Search(k); found {
			fmt.Printf("找到键 %d，值为: %v\n", k, v)
		}
	}

	db := f.NewSimpleDB("users.db")

	for i := range 10 {
		db.Insert(int32(i), "alice"+strconv.Itoa(i))
	}

	// 3. 查询记录并检查是否为nil
	record, err := db.Select(1)
	if err != nil {
		log.Fatalf("Failed to select record: %v", err)
	}

	// 4. 检查record是否为nil
	if record == nil {
		fmt.Println("Record not found")
		return
	}

	fmt.Printf("ID: %d, Name: %s\n", record.ID, record.Name)
}

func listenConn() {
	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", PORT))
	if err != nil {
		fmt.Println("Error happen", err)
		return
	}
	fmt.Println("Server is listening on port", PORT)
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error happen", err)
			continue
		}
		fmt.Println("New Connection happened:", conn.RemoteAddr())
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	fmt.Println("Handling connection from:", conn.RemoteAddr())

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading:", err)
			return
		}

		// 判断是否为 "q" 以退出
		if line == "q\r\n" {
			fmt.Println("Received 'q', exiting...")
			return
		}

		fmt.Printf("Received: %s", line)
		writer.WriteString("Echo: " + line)
		writer.Flush()
	}
}
