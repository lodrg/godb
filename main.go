package main

import (
	"bufio"
	"fmt"
	"godb/tree"
	"net"
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
