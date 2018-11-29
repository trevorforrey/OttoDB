package main

import (
	"OttoDB/server/store/binTree"
	"OttoDB/server/transactionManagers"
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync/atomic"

	"github.com/golang/protobuf/proto"

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

type length int64

var (
	tree               = binTree.NewTree()
	transactionID      = uint64(1)
	transactionManager = transactionManagers.NewClientMap()
	activeTransactions = transactionManagers.NewActiveTxnMap()
	transactionMap     = NewTransactionMap()
	endianness         = binary.LittleEndian
)

const walPath = "./store.pb"
const sizeOfLength = 8

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	addr := ":8080"

	lastTxn, err := replayLog(tree)
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

			var transaction Transaction
			var singleRunTxn bool
			if !inTransaction {
				// Give new transaction a new transaction id
				txID = atomic.AddUint64(&transactionID, 1)
				fmt.Printf("Got a request from a non-transactioned client: %d\n", txID)
				singleRunTxn = true
				// Create a transaction obj for single run txn
				transaction = NewTransaction(txID)
			} else {
				fmt.Printf("Got a request from a transactioned client: %d\n", txID)
				singleRunTxn = false
				// Grab the current txn obj for the txn
				transactionMap.RLock()
				transaction = transactionMap.Transactions[txID]
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
				writeToLog(operation, txID)
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
					writeAbortToLog(txID)
					transaction.Abort()
					removeTxnData(txID, activeTransactions)
					removeClientData(client, transactionManager)
					conn.WriteError("Txn Aborted: " + err.Error())
					return
				}
				if expiredRecord != nil {
					transaction.deletedRecords = append(transaction.deletedRecords, expiredRecord)
				}

				insertedRecord, err := tree.Set(string(cmd.Args[1]), string(cmd.Args[2]), txID, activeTxdSnapshot)
				if err != nil {
					writeAbortToLog(txID)
					transaction.Abort()
					removeTxnData(txID, activeTransactions)
					removeClientData(client, transactionManager)
					conn.WriteError("Txn Aborted: " + err.Error())
					return
				}
				transaction.insertedRecords = append(transaction.insertedRecords, insertedRecord)

				if singleRunTxn {
					activeTransactions.Lock()
					delete(activeTransactions.ActiveTransactions, txID)
					activeTransactions.Unlock()
				}

				transactionMap.Lock()
				transactionMap.Transactions[transaction.timestamp] = transaction
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
					writeAbortToLog(txID)
					transaction.Abort()
					removeTxnData(txID, activeTransactions)
					removeClientData(client, transactionManager)
					conn.WriteError("Txn Aborted: " + err.Error())
					return
				}
				if expiredRecord == nil {
					conn.WriteNull()
					return
				}

				transaction.deletedRecords = append(transaction.deletedRecords, expiredRecord)

				if singleRunTxn {
					activeTransactions.Lock()
					defer activeTransactions.Unlock()
					delete(activeTransactions.ActiveTransactions, txID)
				}

				transactionMap.Lock()
				transactionMap.Transactions[transaction.timestamp] = transaction
				transactionMap.Unlock()
				conn.WriteString("OK")

			case "begin":
				transactionManager.Lock()
				defer transactionManager.Unlock()
				transactionManager.Transactions[client] = txID

				transactionMap.Lock()
				defer transactionMap.Unlock()
				transactionMap.Transactions[txID] = transaction

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
				conn.WriteString(transaction.String())

			case "abort":
				writeAbortToLog(txID)
				// Abort the txn
				transaction.Abort()

				// Remove txn from active txns and client mapping txns
				removeTxnData(txID, activeTransactions)
				removeClientData(client, transactionManager)

				conn.WriteError("Aborted txn from manual client call")

			case "printw":
				if err := printWal(); err != nil {
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

func turnToOp(cmd redcon.Command, txID uint64) (*Operation, error) {
	commandSize := len(cmd.Args)
	switch commandSize {
	case 1:
		return &Operation{
			TxID: txID,
			Op:   string(cmd.Args[0]),
		}, nil
	case 2:
		return &Operation{
			TxID: txID,
			Op:   string(cmd.Args[0]),
			Key:  string(cmd.Args[1]),
		}, nil
	case 3:
		return &Operation{
			TxID:  txID,
			Op:    string(cmd.Args[0]),
			Key:   string(cmd.Args[1]),
			Value: string(cmd.Args[2]),
		}, nil
	default:
		return nil, fmt.Errorf("Unsupported command provided")
	}
}

func writeToLog(operation *Operation, txID uint64) error {
	b, err := proto.Marshal(operation)
	if err != nil {
		return fmt.Errorf("could not encode operation: %v", err)
	}

	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return fmt.Errorf("could not open %s: %v", walPath, err)
	}

	if err := binary.Write(f, endianness, length(len(b))); err != nil {
		return fmt.Errorf("could not enocde length of message: %v", err)
	}
	_, err = f.Write(b)
	if err != nil {
		return fmt.Errorf("could not write task to file: %v", err)
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("could not close file %s: %v", walPath, err)
	}
	return nil
}

func writeAbortToLog(txID uint64) error {
	operation := &Operation{
		TxID: txID,
		Op:   "abort",
	}

	err := writeToLog(operation, txID)
	if err != nil {
		return fmt.Errorf("error writing abort to log: %v", err)
	}
	return nil
}

func printWal() error {
	b, err := ioutil.ReadFile(walPath)
	if err != nil {
		return fmt.Errorf("could not read %s: %v", walPath, err)
	}

	for {
		if len(b) == 0 {
			return nil
		} else if len(b) < sizeOfLength {
			return fmt.Errorf("bytes not correct size")
		}

		var l length
		if err := binary.Read(bytes.NewReader(b[:sizeOfLength]), endianness, &l); err != nil {
			return fmt.Errorf("could not decode message length: %v", err)
		}
		b = b[sizeOfLength:]

		var operation Operation
		if err := proto.Unmarshal(b[:l], &operation); err != nil {
			return fmt.Errorf("Could not read operation: %v", err)
		}
		b = b[l:]

		fmt.Printf("Txn: %d,\tOp: %s\tKey: %s\tVal: %s\n", operation.TxID, operation.Op, operation.Key, operation.Value)
	}
}

func replayLog(tree *binTree.BinTree) (uint64, error) {

	if _, err := os.Stat(walPath); os.IsNotExist(err) {
		return 0, nil
	}

	b, err := ioutil.ReadFile(walPath)
	if err != nil {
		return 0, fmt.Errorf("could not read %s: %v", walPath, err)
	}

	transactionMap := NewTransactionMap()
	var lastTxn uint64

	for {
		if len(b) == 0 {
			break
		} else if len(b) < sizeOfLength {
			return 0, fmt.Errorf("bytes not correct size")
		}

		var l length
		if err := binary.Read(bytes.NewReader(b[:sizeOfLength]), endianness, &l); err != nil {
			return 0, fmt.Errorf("could not decode message length: %v", err)
		}
		b = b[sizeOfLength:]

		var operation Operation
		if err := proto.Unmarshal(b[:l], &operation); err != nil {
			return 0, fmt.Errorf("Could not read operation: %v", err)
		}
		b = b[l:]

		// Replaying the txn on the in-memory store
		fmt.Printf("Txn: %d,\tOp: %s\tKey: %s\tVal: %s\n", operation.TxID, operation.Op, operation.Key, operation.Value)

		if operation.TxID > lastTxn {
			lastTxn = operation.TxID
		}

		// Get transaction, and determine if one already exists for the txID
		transaction, inTransaction := transactionMap.Transactions[operation.TxID]
		if !inTransaction {
			transaction = NewTransaction(operation.TxID)
		}

		if operation.Op == "abort" {
			delete(transactionMap.Transactions, operation.TxID)
		} else {
			transaction.replayOps = append(transaction.replayOps, operation)
			transactionMap.Transactions[operation.TxID] = transaction
		}
	}

	transactions := make([]uint64, 0)
	for txnID := range transactionMap.Transactions {
		transactions = append(transactions, txnID)
	}
	sort.Slice(transactions, func(i, j int) bool { return transactions[i] < transactions[j] })
	for _, transactionID := range transactions {
		fmt.Printf("About to batch perform txn: %d", transactionID)
		txn := transactionMap.Transactions[transactionID]
		err := txn.BatchExecute(tree)
		if err != nil {
			return 0, err
		}
	}
	return lastTxn, nil
}
