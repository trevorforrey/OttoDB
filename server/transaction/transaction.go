package transaction

import (
	"OttoDB/server/oplog/logprotobuf"
	"OttoDB/server/store/record"
	fmt "fmt"
	"strconv"
	"strings"
	"sync"
)

type Transaction struct {
	Timestamp       uint64
	InsertedRecords []*record.Record
	DeletedRecords  []*record.Record
	ReplayOps       []logprotobuf.Operation
	RWAntiDepIn     int8
	RWAntiDepOut    int8
}

type TransactionMap struct {
	sync.RWMutex
	Transactions map[uint64]*Transaction
}

func NewTransactionMap() *TransactionMap {
	return &TransactionMap{Transactions: make(map[uint64]*Transaction)}
}

func (txnMap *TransactionMap) AddRWAntiDepFlags(outTxn uint64, inTxn uint64) error {
	txnMap.Transactions[outTxn].RWAntiDepOut = 1
	txnMap.Transactions[inTxn].RWAntiDepIn = 1
	return nil
}

func NewTransaction(timestamp uint64) *Transaction {
	return &Transaction{Timestamp: timestamp, InsertedRecords: make([]*record.Record, 0), DeletedRecords: make([]*record.Record, 0)}
}

func (txn *Transaction) Abort() {
	// reset all expiration dates to old ones
	for _, delRcrd := range txn.DeletedRecords {
		delRcrd.ExpiredBy = delRcrd.OldExpiredBy
	}

	fmt.Printf("Inserted record size: %d", len(txn.InsertedRecords))

	// set inserted nodes as aborted
	for _, rcrd := range txn.InsertedRecords {
		rcrd.Status = record.Aborted
	}
}

func (txn *Transaction) String() string {
	var sb strings.Builder

	sb.WriteString("Txn: ")
	sb.WriteString(strconv.Itoa(int(txn.Timestamp)))
	sb.WriteString("   |")
	sb.WriteString("\n")

	sb.WriteString("RW Anti Deps: ")
	sb.WriteString("in: ")
	sb.WriteString(strconv.Itoa(int(txn.RWAntiDepIn)))
	sb.WriteString("   |")
	sb.WriteString("out: ")
	sb.WriteString(strconv.Itoa(int(txn.RWAntiDepOut)))
	sb.WriteString("   |")
	sb.WriteString("\n")

	expiredRecords := txn.DeletedRecords
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
	insertedRecords := txn.InsertedRecords
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
