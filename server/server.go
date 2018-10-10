package main

import (
	"bufio"
	"fmt"
	"net"
	"strings"
)

func main() {
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Printf("Error while getting connection %g", err)
	}
	defer ln.Close()
	for {
		conn, err := ln.Accept()
		fmt.Printf("Recieved a request")
		if err != nil {
			fmt.Printf("Error while accepting connection %s", err)
		}
		go handleRequest(conn)
	}
}

func handleRequest(conn net.Conn) {
	fmt.Println("About to handle request")
	fullOp, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		fmt.Printf("Error reading message from client\n")
	}
	fmt.Printf("Received: %s", fullOp)
	fmt.Fprint(conn, "Request received\n")
	ops := strings.Fields(fullOp)
	fmt.Println(ops)
	conn.Close()
}
