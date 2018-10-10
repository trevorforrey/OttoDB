package main

import (
	"OttoDB/server/rbTree"
	"bufio"
	"errors"
	"fmt"
	"net"
	"strings"
)

type operation struct {
	op    string
	key   string
	value string
}

var (
	tree     = rbTree.NewTree()
	validOps = map[string]struct{}{"GET": {}, "SET": {}, "DEL": {}, "QUIT": {}, "BEGIN": {}, "COMMIT": {}}
)

func main() {
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Printf("Error while getting connection %g", err)
	}
	defer ln.Close()
	for {
		conn, err := ln.Accept()
		fmt.Println("Recieved a request")
		if err != nil {
			fmt.Printf("Error while accepting connection %s", err)
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	fmt.Println("About to handle request")
	defer conn.Close()
	fullOp, err := bufio.NewReader(conn).ReadString('\n')
	operation, err := parseOp(fullOp)
	if err != nil {
		fmt.Printf("Error reading message from client: %s\n", err)
	}
	response, err := performOp(operation)
	if err != nil {
		fmt.Printf("Error performing operation %s\n", err)
	}
	fmt.Println("About to send response")
	fmt.Fprint(conn, response)
}

func parseOp(fullOp string) (operation, error) {
	splitOps := strings.Fields(fullOp)
	var newOp operation
	newOp.op = splitOps[0]
	newOp.key = splitOps[1]
	if newOp.op == "SET" {
		newOp.value = splitOps[2]
	}
	fmt.Printf("Received %s operaiton\n", newOp.op)

	// Check that new op is a valid op, if not, return error
	_, valid := validOps[newOp.op]
	if !valid {
		return newOp, errors.New("Invalid operation requested: ")
	}
	return newOp, nil
}

func performOp(op operation) (string, error) {
	if op.op == "GET" {
		fmt.Println("About to perform get request")
		keyVal := tree.Get(op.key)
		fmt.Printf("Retrieved %s from tree\n", keyVal)
		return keyVal, nil
	} else if op.op == "SET" {
		fmt.Println("About to perform set request")
		fmt.Printf("Key: %s\n", op.key)
		fmt.Printf("Value: %s\n", op.value)
		tree.Set(op.key, op.value)
		tree.InOrderTraversal()
		return "set value in db", nil
	}
	return "operation didn't match", errors.New("Op didn't match")
}
