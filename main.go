package main

import (
	"bufio"
	"fmt"
	"godb/disktree"
	"godb/logger"
	"log"
	"net"
	"strconv"
)

const PORT = 8088

func main() {
	//useBP()

	//p.Test()
}

func useBP() {
	dbfileName := "test.db"
	logFileName := "test.log"
	redoLog, _ := disktree.NewRedoLog(logFileName)

	diskPager, err := disktree.NewDiskPager(dbfileName, 80, 80, redoLog)

	if err != nil {
		log.Fatal("Failed to allocate new page")
	}
	// 创建一个4阶B+树
	tree := disktree.NewBPTree(4, 10, diskPager, redoLog)

	// 插入测试数据
	testData := map[uint32]string{
		1:  "一",  // One
		2:  "二",  // Two
		3:  "三",  // Three
		4:  "四",  // Four
		5:  "五",  // Five
		6:  "六",  // Six
		7:  "七",  // Seven
		8:  "八",  // Eight
		9:  "九",  // Nine
		10: "十",  // Ten
		11: "十一", // Eleven
		12: "十二", // Twelve
		13: "十三", // Thirteen
		14: "十四", // Fourteen
		15: "十五", // Fifteen
		16: "十六", // Sixteen
		17: "十七", // Seventeen
		18: "十八", // Eighteen
		19: "十九", // Nineteen
		20: "二十", // Twenty
	}

	// 插入数据并打印树的状态
	for k, v := range testData {
		tree.Insert(k, []byte(v))
		fmt.Printf("\n插入 %d:%s 后的树结构:\n", k, v)
		tree.Print()
	}

	// 搜索测试
	fmt.Println("\n搜索测试:")
	for k := 1; k <= 10; k++ {
		if v, found := tree.Search(uint32(k)); found {
			fmt.Printf("找到键 %d，值为: %s\n", k, v)
		}
	}
	search, _ := tree.Search(3)
	fmt.Println("search:", search)

	db := disktree.NewSimpleDB("users.db")

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
		logger.Debug("Record not found")
		return
	}

	logger.Debug("ID: %d, Name: %s\n", record.ID, record.Name)
}

func listenConn() {
	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", PORT))
	if err != nil {
		fmt.Println("Error happen", err)
		return
	}
	logger.Debug("Server is listening on port", PORT)
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
