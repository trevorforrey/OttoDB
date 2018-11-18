package main

import (
	"OttoDB/server/store/binTree"
	fmt "fmt"
	"strconv"
	"strings"
	"sync"
)

type Transaction struct {
	timestamp                        uint64
	insertedRecords                  []*binTree.Record
	deletedRecords                   []*binTree.Record
	deletedRecordsPreviousExpiration []uint64
}

type TransactionMap struct {
	sync.RWMutex
	Transactions map[uint64]Transaction
}

func NewTransactionMap() *TransactionMap {
	return &TransactionMap{Transactions: make(map[uint64]Transaction)}
}

func NewTransaction(timestamp uint64) Transaction {
	return Transaction{timestamp: timestamp, insertedRecords: make([]*binTree.Record, 0), deletedRecords: make([]*binTree.Record, 0)}
}

func (txn *Transaction) Abort() {
	// reset all expiration dates to old ones
	for _, record := range txn.deletedRecords {
		record.ExpiredBy = record.OldExpiredBy
	}

	// set inserted nodes as aborted
	for _, record := range txn.insertedRecords {
		record.Status = binTree.Aborted
	}
}

func (txn *Transaction) String() string {
	var sb strings.Builder
	expiredRecords := txn.deletedRecords
	sb.WriteString("deleted records     ")
	for _, record := range expiredRecords {
		sb.WriteString("value: ")
		sb.WriteString(record.Value)
		sb.WriteString("   |")

		sb.WriteString("created: ")
		sb.WriteString(strconv.Itoa(int(record.CreatedBy)))
		sb.WriteString("   |")

		sb.WriteString("expired: ")
		sb.WriteString(strconv.Itoa(int(record.ExpiredBy)))
		sb.WriteString("   |")
		sb.WriteString("\n")

		sb.WriteString("status: ")
		sb.WriteString(strconv.Itoa(int(record.Status)))
		sb.WriteString("   |")
		sb.WriteString("\n")
	}
	insertedRecords := txn.insertedRecords
	sb.WriteString("inserted records     ")
	for _, record := range insertedRecords {
		sb.WriteString("value: ")
		sb.WriteString(record.Value)
		sb.WriteString("   |")

		sb.WriteString("created: ")
		sb.WriteString(strconv.Itoa(int(record.CreatedBy)))
		sb.WriteString("   |")

		sb.WriteString("expired: ")
		sb.WriteString(strconv.Itoa(int(record.ExpiredBy)))
		sb.WriteString("   |")
		sb.WriteString("\n")

		sb.WriteString("status: ")
		sb.WriteString(strconv.Itoa(int(record.Status)))
		sb.WriteString("   |")
		sb.WriteString("\n")
	}
	return sb.String()
}

func (txn *Transaction) Execute(tree *binTree.BinTree, operation Operation) error {
	switch operation.Op {
	case "set":
		expiredRecord, err := tree.ExpireReplay(operation.Key, operation.TxID)
		if err != nil {
			txn.Abort()
			return fmt.Errorf("Ran into an error whil expiring key: %s on txn: %d", operation.Key, operation.TxID)
		}
		txn.deletedRecords = append(txn.deletedRecords, expiredRecord)

		insertedRecord, err := tree.SetReplay(operation.Key, operation.Value, operation.TxID)
		if err != nil {
			return fmt.Errorf("Ran into error while setting key: %s on txn: %d", operation.Key, operation.TxID)
		}
		txn.insertedRecords = append(txn.insertedRecords, insertedRecord)

		return nil

	case "del":
		expiredRecord, err := tree.ExpireReplay(operation.Key, operation.TxID)
		if err != nil {
			txn.Abort()
			return fmt.Errorf("Ran into an error whil expiring key: %s on txn: %d", operation.Key, operation.TxID)
		}
		txn.deletedRecords = append(txn.deletedRecords, expiredRecord)

		return nil
	default:
		return nil
	}
}
