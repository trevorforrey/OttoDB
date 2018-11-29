package main

import (
	"OttoDB/server/oplog"
	"OttoDB/server/oplog/logprotobuf"
	"OttoDB/server/store/binTree"
	"OttoDB/server/transaction"
	"OttoDB/server/transactionManagers"
	"fmt"
	"log"
	"runtime"
	"strings"
	"sync/atomic"

	"github.com/tidwall/redcon"
)

type store interface {
	Get() string
	Set() string
	Del() string
}

var (
	tree               = binTree.NewTree()
	transactionID      = uint64(1)
	transactionManager = transactionManagers.NewClientMap()
	activeTransactions = transactionManagers.NewActiveTxnMap()
	transactionMap     = transaction.NewTransactionMap()
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	addr := ":8080"

	lastTxn, err := oplog.ReplayLog(tree)
	if err != nil {
		fmt.Printf("Error while replaying log: %v", err)
	}
	transactionID = lastTxn + 1

	err = redcon.ListenAndServe(addr,
		func(conn redcon.Conn, cmd redcon.Command) {

			client := conn.NetConn().RemoteAddr().String()

			// Start Transaction, get txID
			transactionManager.RLock()
			txID, inTransaction := transactionManager.Transactions[client]
			transactionManager.RUnlock()

			var txn transaction.Transaction
			var singleRunTxn bool
			if !inTransaction {
				// Give new transaction a new transaction id
				txID = atomic.AddUint64(&transactionID, 1)
				fmt.Printf("Got a request from a non-transactioned client: %d\n", txID)
				singleRunTxn = true
				// Create a transaction obj for single run txn
				txn = transaction.NewTransaction(txID)
			} else {
				fmt.Printf("Got a request from a transactioned client: %d\n", txID)
				singleRunTxn = false
				// Grab the current txn obj for the txn
				transactionMap.RLock()
				txn = transactionMap.Transactions[txID]
				transactionMap.RUnlock()
			}
			activeTransactions.Lock()
			activeTransactions.ActiveTransactions[txID] = true
			activeTransactions.Unlock()

			activeTransactions.RLock()
			activeTxdSnapshot := shapshotActiveTransactions(activeTransactions.ActiveTransactions)
			activeTransactions.RUnlock()

			operation, err := turnToOp(cmd, txID)
			if err == nil {
				oplog.WriteToLog(operation, txID)
			}

			switch strings.ToLower(string(cmd.Args[0])) {
			default:
				conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")

			case "ping":
				conn.WriteString("PONG")

			case "quit":
				removeTxnData(txID, activeTransactions)
				removeClientData(client, transactionManager)

				conn.WriteString("OK")
				conn.Close()

			case "set":
				if len(cmd.Args) != 3 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}

				expiredRecord, err := tree.Expire(string(cmd.Args[1]), txID, activeTxdSnapshot)
				if err != nil {
					oplog.WriteAbortToLog(txID)
					txn.Abort()
					removeTxnData(txID, activeTransactions)
					removeClientData(client, transactionManager)
					conn.WriteError("Txn Aborted: " + err.Error())
					return
				}
				if expiredRecord != nil {
					txn.DeletedRecords = append(txn.DeletedRecords, expiredRecord)
				}

				insertedRecord, err := tree.Set(string(cmd.Args[1]), string(cmd.Args[2]), txID, activeTxdSnapshot)
				if err != nil {
					oplog.WriteAbortToLog(txID)
					txn.Abort()
					removeTxnData(txID, activeTransactions)
					removeClientData(client, transactionManager)
					conn.WriteError("Txn Aborted: " + err.Error())
					return
				}
				txn.InsertedRecords = append(txn.InsertedRecords, insertedRecord)

				if singleRunTxn {
					activeTransactions.Lock()
					delete(activeTransactions.ActiveTransactions, txID)
					activeTransactions.Unlock()
				}

				transactionMap.Lock()
				transactionMap.Transactions[txn.Timestamp] = txn
				transactionMap.Unlock()
				conn.WriteString("OK")

			case "get":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				keyVal, err := tree.Get(string(cmd.Args[1]), txID, activeTxdSnapshot)
				if err != nil {
					fmt.Print(err)
					conn.WriteNull()
					return
				} else if singleRunTxn {
					activeTransactions.Lock()
					delete(activeTransactions.ActiveTransactions, txID)
					activeTransactions.Unlock()
				}
				conn.WriteString(keyVal)

			case "del":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				expiredRecord, err := tree.Expire(string(cmd.Args[1]), txID, activeTxdSnapshot)
				if err != nil {
					oplog.WriteAbortToLog(txID)
					txn.Abort()
					removeTxnData(txID, activeTransactions)
					removeClientData(client, transactionManager)
					conn.WriteError("Txn Aborted: " + err.Error())
					return
				}
				if expiredRecord == nil {
					conn.WriteNull()
					return
				}

				txn.DeletedRecords = append(txn.DeletedRecords, expiredRecord)

				if singleRunTxn {
					activeTransactions.Lock()
					defer activeTransactions.Unlock()
					delete(activeTransactions.ActiveTransactions, txID)
				}

				transactionMap.Lock()
				transactionMap.Transactions[txn.Timestamp] = txn
				transactionMap.Unlock()
				conn.WriteString("OK")

			case "begin":
				transactionManager.Lock()
				defer transactionManager.Unlock()
				transactionManager.Transactions[client] = txID

				transactionMap.Lock()
				defer transactionMap.Unlock()
				transactionMap.Transactions[txID] = txn

				conn.WriteString("OK")

			case "commit":
				activeTransactions.Lock()
				defer activeTransactions.Unlock()
				delete(activeTransactions.ActiveTransactions, txID)

				transactionManager.Lock()
				defer transactionManager.Unlock()
				delete(transactionManager.Transactions, client)
				conn.WriteString("OK")

			case "print":
				nodeTimeStamps := tree.RecordListPrint(string(cmd.Args[1]))
				conn.WriteString(nodeTimeStamps)

			case "txnprint":
				conn.WriteString(txn.String())

			case "abort":
				oplog.WriteAbortToLog(txID)
				// Abort the txn
				txn.Abort()

				// Remove txn from active txns and client mapping txns
				removeTxnData(txID, activeTransactions)
				removeClientData(client, transactionManager)

				conn.WriteError("Aborted txn from manual client call")

			case "printw":
				if err := oplog.PrintWal(); err != nil {
					fmt.Printf(err.Error())
				}
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

func removeClientData(client string, transactionManager *transactionManagers.ClientTxdMap) {
	transactionManager.Lock()
	defer transactionManager.Unlock()
	delete(transactionManager.Transactions, client)
}

func removeTxnData(txID uint64, activeTransactions *transactionManagers.ActiveTxdMap) {
	activeTransactions.Lock()
	defer activeTransactions.Unlock()
	delete(activeTransactions.ActiveTransactions, txID)
}

func turnToOp(cmd redcon.Command, txID uint64) (*logprotobuf.Operation, error) {
	commandSize := len(cmd.Args)
	switch commandSize {
	case 1:
		return &logprotobuf.Operation{
			TxID: txID,
			Op:   string(cmd.Args[0]),
		}, nil
	case 2:
		return &logprotobuf.Operation{
			TxID: txID,
			Op:   string(cmd.Args[0]),
			Key:  string(cmd.Args[1]),
		}, nil
	case 3:
		return &logprotobuf.Operation{
			TxID:  txID,
			Op:    string(cmd.Args[0]),
			Key:   string(cmd.Args[1]),
			Value: string(cmd.Args[2]),
		}, nil
	default:
		return nil, fmt.Errorf("Unsupported command provided")
	}
}
