package main

import (
    "fmt"
    "net"
    "bufio"
)

const PORT = 8088

func main() {
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
