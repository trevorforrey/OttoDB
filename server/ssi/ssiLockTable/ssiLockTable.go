package ssiLockTable

import "sync"

type txnStatus int

const (
	InProgress txnStatus = iota
	Aborted
	Committed
)

type TxnReadLockDetails struct {
	BlockingTxns []uint64
	Status       txnStatus
}

type SIReadKeyLockTable struct {
	sync.RWMutex
	Table map[string][]uint64
}

type SIReadTxnDetailsTable struct {
	Details map[uint64]TxnReadLockDetails
}

func NewSIReadKeyLockTable() *SIReadKeyLockTable {
	return &SIReadKeyLockTable{Table: make(map[string][]uint64)}
}

func NewSIReadTxnDetailsTable() *SIReadTxnDetailsTable {
	return &SIReadTxnDetailsTable{Details: make(map[uint64]TxnReadLockDetails)}
}
