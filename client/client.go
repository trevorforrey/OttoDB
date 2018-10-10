package main

import (
	"bufio"
	"fmt"
	"net"
)

func main() {
	conn, err := net.Dial("tcp", "127.0.0.1:8080")
	if err != nil {
		fmt.Println("Error connecting to server")
	}
	request := "Get key1\r\n"
	fmt.Fprint(conn, request)

	message, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		fmt.Println("Error getting response back from server")
	}
	fmt.Printf("Server Response: %s", message)

	conn, err = net.Dial("tcp", "127.0.0.1:8080")
	if err != nil {
		fmt.Println("Error connecting to server")
	}

	request = "Get key2\r\n"
	fmt.Println("About to send a request")
	fmt.Fprint(conn, request)
	fmt.Println("Request sent")

	message1, err := bufio.NewReader(conn).ReadString('\n')
	if err != nil {
		fmt.Println("Error getting response back from server")
	}
	fmt.Printf("Server Response: %s", message1)
}
