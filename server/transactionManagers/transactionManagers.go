package transactionManagers

import "sync"

type ClientTxdMap struct {
	sync.RWMutex
	Transactions map[string]uint64
}

type ActiveTxdMap struct {
	sync.RWMutex
	ActiveTransactions map[uint64]bool
}

func NewClientMap() *ClientTxdMap {
	return &ClientTxdMap{Transactions: make(map[string]uint64)}
}

func NewActiveTxnMap() *ActiveTxdMap {
	return &ActiveTxdMap{ActiveTransactions: make(map[uint64]bool)}
}
