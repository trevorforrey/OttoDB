package main

import (
	"OttoDB/server/rbTree"
	"OttoDB/server/transactionManagers"
	"bufio"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
)

type operation struct {
	op    string
	key   string
	value string
}

var (
	tree               = rbTree.NewTree()
	validOps           = map[string]struct{}{"GET": {}, "SET": {}, "DEL": {}, "QUIT": {}, "BEGIN": {}, "COMMIT": {}}
	transactionID      uint64
	transactionManager = transactionManagers.NewClientMap()
	activeTransactions = transactionManagers.NewActiveTxnMap()
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
		fmt.Printf("From Remote: %v\n", conn.RemoteAddr())
		if err != nil {
			fmt.Printf("Error while accepting connection %s", err)
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	fmt.Println("About to handle connection")
	defer conn.Close()
	for {
		fullOp, err := bufio.NewReader(conn).ReadString('\n')
		operation, err := parseOp(fullOp)
		if err != nil {
			fmt.Printf("Error reading message from client: %s\n", err)
		}
		response, err := performOp(operation, conn.RemoteAddr().String())
		if err != nil {
			fmt.Printf("Error performing operation %s\n", err)
		}
		if response == "connection closed\n" {
			fmt.Printf("About to send response: %s\n", response)
			fmt.Fprint(conn, response)
			conn.Close()
			break
		} else {
			fmt.Printf("About to send response: %s\n", response)
			fmt.Fprint(conn, response)
		}
	}
	fmt.Println("closed connection with client")
}

func parseOp(fullOp string) (operation, error) {
	splitOps := strings.Fields(fullOp)
	var newOp operation
	newOp.op = splitOps[0]
	if newOp.op == "GET" || newOp.op == "SET" || newOp.op == "DEL" {
		newOp.key = splitOps[1]
	}
	if newOp.op == "SET" {
		newOp.value = splitOps[2]
	}
	fmt.Printf("Received %s operation\n", newOp.op)

	// Check that new op is a valid op, if not, return error
	_, valid := validOps[newOp.op]
	if !valid {
		return newOp, errors.New("Invalid operation requested: ")
	}
	return newOp, nil
}

func performOp(op operation, client string) (string, error) {
	// Start Transaction, get txID
	transactionManager.RLock()
	txID, inTransaction := transactionManager.Transactions[client]
	transactionManager.RUnlock()

	singleRunTxn := true
	if !inTransaction {
		atomic.AddUint64(&transactionID, 1)
		txID = transactionID
		fmt.Printf("Got a request from a non-transactioned client: %d\n", txID)
	} else {
		singleRunTxn = false
		fmt.Printf("Got a request from a transactioned client: %d\n", txID)
	}
	activeTransactions.Lock()
	activeTransactions.ActiveTransactions[txID] = true
	activeTransactions.Unlock()

	activeTransactions.RLock()
	activeTxdSnapshot := shapshotActiveTransactions(activeTransactions.ActiveTransactions)
	activeTransactions.RUnlock()

	// If in a long transaction, scope to that timestamp
	if op.op == "BEGIN" {
		transactionManager.Lock()
		transactionManager.Transactions[client] = txID
		transactionManager.Unlock()
		return "started a new transaction\n", nil

	} else if op.op == "GET" {
		var sb strings.Builder
		fmt.Println("About to perform get request")
		keyVal, err := tree.Get(op.key, txID, activeTxdSnapshot)
		if err != nil {
			return err.Error(), err
		}
		if singleRunTxn {
			delete(activeTransactions.ActiveTransactions, txID)
		}
		fmt.Printf("Retrieved %s from tree\n", keyVal)
		sb.WriteString(keyVal)
		sb.WriteString("\n")
		return sb.String(), nil

	} else if op.op == "SET" {
		fmt.Println("About to perform set request")
		fmt.Printf("Key: %s\n", op.key)
		fmt.Printf("Value: %s\n", op.value)
		tree.Set(op.key, op.value, txID, activeTxdSnapshot)
		if singleRunTxn {
			delete(activeTransactions.ActiveTransactions, txID)
		}
		tree.InOrderTraversal()
		return "set value in db\n", nil

	} else if op.op == "DEL" {
		fmt.Println("About to perform delete request")
		fmt.Printf("Key: %s\n", op.key)
		err := tree.Expire(op.key, txID, activeTxdSnapshot)
		if singleRunTxn {
			delete(activeTransactions.ActiveTransactions, txID)
		}
		if err != nil {
			return err.Error(), err
		}
		tree.InOrderTraversal()
		return "deleted key in db\n", nil

	} else if op.op == "QUIT" {
		fmt.Println("About to quit and close connection")
		delete(activeTransactions.ActiveTransactions, txID)
		delete(transactionManager.Transactions, client)
		return "connection closed\n", nil
	}
	return "operation didn't match\n", errors.New("Op didn't match")
}

func shapshotActiveTransactions(supermap map[uint64]bool) map[uint64]bool {
	resultMap := make(map[uint64]bool)
	for key, value := range supermap {
		resultMap[key] = value
	}
	return resultMap
}
