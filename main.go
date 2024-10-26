package main

import (
    "fmt"
    "net"
    "bufio"
    "godb/tree"
)

const PORT = 8088

func main() {
    //listenConn()

    root := tree.NewTreeNode(1)


    child1 := tree.NewTreeNode(2)
    child2 := tree.NewTreeNode(3)
    child3 := tree.NewTreeNode(4)

    root.AddChild(child1)
    root.AddChild(child2)

    child1.AddChild(tree.NewTreeNode(5))
    child1.AddChild(tree.NewTreeNode(6))

    child2.AddChild(child3)

    fmt.Println("树结构打印：")
    root.PrintTree(0)

}

func listenConn(){
       listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d",PORT))
    if (err != nil) {
        fmt.Println("Error happen", err)
        return
    }
    fmt.Println("Server is listening on port", PORT)
    defer listener.Close()

    for {
        conn, err := listener.Accept()
        if (err != nil) {
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
