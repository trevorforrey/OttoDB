package oplog

import (
	"OttoDB/server/oplog/logprotobuf"
	"OttoDB/server/store/binTree"
	"OttoDB/server/transaction"
	"bytes"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"os"
	"sort"

	"github.com/golang/protobuf/proto"
)

type length int64

const walPath = "./store.pb"
const sizeOfLength = 8

var endianness = binary.LittleEndian

func WriteToLog(operation *logprotobuf.Operation, txID uint64) error {
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

func WriteAbortToLog(txID uint64) error {
	operation := &logprotobuf.Operation{
		TxID: txID,
		Op:   "abort",
	}

	err := WriteToLog(operation, txID)
	if err != nil {
		return fmt.Errorf("error writing abort to log: %v", err)
	}
	return nil
}

func PrintWal() error {
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

		var operation logprotobuf.Operation
		if err := proto.Unmarshal(b[:l], &operation); err != nil {
			return fmt.Errorf("Could not read operation: %v", err)
		}
		b = b[l:]

		fmt.Printf("Txn: %d,\tOp: %s\tKey: %s\tVal: %s\n", operation.TxID, operation.Op, operation.Key, operation.Value)
	}
}

func ReplayLog(tree *binTree.BinTree) (uint64, error) {

	if _, err := os.Stat(walPath); os.IsNotExist(err) {
		return 0, nil
	}

	b, err := ioutil.ReadFile(walPath)
	if err != nil {
		return 0, fmt.Errorf("could not read %s: %v", walPath, err)
	}

	transactionMap := transaction.NewTransactionMap()
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

		var operation logprotobuf.Operation
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
		var txn *transaction.Transaction
		txn, inTransaction := transactionMap.Transactions[operation.TxID]
		if !inTransaction {
			txn = transaction.NewTransaction(operation.TxID)
		}

		if operation.Op == "abort" {
			delete(transactionMap.Transactions, operation.TxID)
		} else {
			txn.ReplayOps = append(txn.ReplayOps, operation)
			transactionMap.Transactions[operation.TxID] = txn
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
		err := BatchExecute(txn, tree)
		if err != nil {
			return 0, err
		}
	}
	return lastTxn, nil
}

func Execute(tree *binTree.BinTree, txn *transaction.Transaction, operation logprotobuf.Operation) error {
	switch operation.Op {
	case "set":
		expiredRecord, err := tree.ExpireReplay(operation.Key, operation.TxID)
		if err != nil {
			txn.Abort()
			return fmt.Errorf("Ran into an error whil expiring key: %s on txn: %d", operation.Key, operation.TxID)
		}
		if expiredRecord != nil {
			txn.DeletedRecords = append(txn.DeletedRecords, expiredRecord)
		}

		insertedRecord, err := tree.SetReplay(operation.Key, operation.Value, operation.TxID)
		if err != nil {
			return fmt.Errorf("Ran into error while setting key: %s on txn: %d", operation.Key, operation.TxID)
		}
		txn.InsertedRecords = append(txn.InsertedRecords, insertedRecord)

		return nil

	case "del":
		expiredRecord, err := tree.ExpireReplay(operation.Key, operation.TxID)
		if err != nil {
			txn.Abort()
			return fmt.Errorf("Ran into an error whil expiring key: %s on txn: %d", operation.Key, operation.TxID)
		}
		txn.DeletedRecords = append(txn.DeletedRecords, expiredRecord)

		return nil
	default:
		return nil
	}
}

func BatchExecute(txn *transaction.Transaction, tree *binTree.BinTree) error {
	for _, operation := range txn.ReplayOps {
		err := Execute(tree, txn, operation)
		if err != nil {
			return fmt.Errorf("Received error during batch execute: %v", err)
		}
	}
	return nil
}
