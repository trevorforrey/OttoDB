package main

import (
	"OttoDB/server/transactionManagers"
	"OttoDB/store/rbTree"
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

type store interface {
	Get() string
	Set() string
	Del() string
}

var (
	tree               = rbTree.NewTree()
	transactionID      uint64
	transactionManager = transactionManagers.NewClientMap()
	activeTransactions = transactionManagers.NewActiveTxnMap()
)

func main() {

	addr := ":8080"

	err := redcon.ListenAndServe(addr,
		func(conn redcon.Conn, cmd redcon.Command) {

			client := conn.NetConn().RemoteAddr().String()

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

			switch strings.ToLower(string(cmd.Args[0])) {
			default:
				conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")

			case "ping":
				conn.WriteString("PONG")

			case "quit":
				delete(activeTransactions.ActiveTransactions, txID)
				delete(transactionManager.Transactions, client)
				conn.WriteString("OK")
				conn.Close()

			case "set":
				if len(cmd.Args) != 3 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				err := tree.Set(string(cmd.Args[1]), string(cmd.Args[2]), txID, activeTxdSnapshot)
				if singleRunTxn {
					delete(activeTransactions.ActiveTransactions, txID)
				}
				if err != nil {
					conn.WriteNull()
				}
				conn.WriteString("OK")

			case "get":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				keyVal, err := tree.Get(string(cmd.Args[1]), txID, activeTxdSnapshot)
				if singleRunTxn {
					delete(activeTransactions.ActiveTransactions, txID)
				}
				if err != nil {
					fmt.Print(err)
					conn.WriteNull()
				} else {
					conn.WriteString(keyVal)
				}

			case "del":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				err := tree.Expire(string(cmd.Args[1]), txID, activeTxdSnapshot)
				if singleRunTxn {
					delete(activeTransactions.ActiveTransactions, txID)
				}
				if err != nil {
					conn.WriteNull()
				} else {
					conn.WriteString("OK")
				}

			case "begin":
				transactionManager.Lock()
				transactionManager.Transactions[client] = txID
				transactionManager.Unlock()
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

func shapshotActiveTransactions(supermap map[uint64]bool) map[uint64]bool {
	resultMap := make(map[uint64]bool)
	for key, value := range supermap {
		resultMap[key] = value
	}
	return resultMap
}
