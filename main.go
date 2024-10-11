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
        conn.Close()
    }
}

