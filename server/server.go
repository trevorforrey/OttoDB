package main

import (
	"OttoDB/server/rbTree"
	"OttoDB/server/transactionManagers"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync/atomic"

	"github.com/tidwall/redcon"
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

	addr := ":8080"

	err := redcon.ListenAndServe(addr,
		func(conn redcon.Conn, cmd redcon.Command) {
			switch strings.ToLower(string(cmd.Args[0])) {
			default:
				conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
			case "ping":
				conn.WriteString("PONG")
			case "quit":
				conn.WriteString("OK")
				conn.Close()
			case "set":
				if len(cmd.Args) != 3 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				conn.WriteString("OK")
			case "get":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				conn.WriteString("OK")
				// if !ok {
				// 	conn.WriteNull()
				// } else {
				// 	conn.WriteBulk(val)
				// }
			case "del":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				conn.WriteString("OK")
			case "begin":
				conn.WriteString("OK")
			case "commit":
				conn.WriteString("OK")
			}
		},
		func(conn redcon.Conn) bool {
			// use this function to accept or deny the connection.
			log.Printf("accept: %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			// this is called when the connection has been closed
			log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
		},
	)
	if err != nil {
		log.Fatal(err)
	}
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
		err := tree.Set(op.key, op.value, txID, activeTxdSnapshot)
		if singleRunTxn {
			delete(activeTransactions.ActiveTransactions, txID)
		}
		if err != nil {
			return err.Error(), err
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
