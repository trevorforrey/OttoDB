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
	defer conn.Close()

	////////////////////////////////////////////////

	fmt.Println("About to send SET request")
	request := "SET key1 bananas\r\n"
	fmt.Fprint(conn, request)

	connBuffer := bufio.NewReader(conn)

	message, err := connBuffer.ReadString('\n')
	if err != nil {
		fmt.Println("Error getting response back from server")
	}
	fmt.Printf("Server Response: %s\n", message)

	////////////////////////////////////////////////

	request = "GET key1\r\n"
	fmt.Println("About to send GET request")
	fmt.Fprint(conn, request)
	fmt.Println("Request sent")

	message, err = connBuffer.ReadString('\n')
	if err != nil {
		fmt.Println("Error getting response back from server")
	}
	fmt.Printf("Server Response: %s", message)

	////////////////////////////////////////////////

	request = "GET key1\r\n"
	fmt.Println("About to send GET request")
	fmt.Fprint(conn, request)
	fmt.Println("Request sent")

	message, err = connBuffer.ReadString('\n')
	if err != nil {
		fmt.Println("Error getting response back from server")
	}
	fmt.Printf("Server Response: %s", message)

	////////////////////////////////////////////////

	request = "QUIT\r\n"
	fmt.Println("About to send QUIT request")
	fmt.Fprint(conn, request)
	fmt.Println("Request sent")

	message, err = connBuffer.ReadString('\n')
	if err != nil {
		fmt.Println("Error getting response back from server")
	}
	fmt.Printf("Server Response: %s", message)

}
