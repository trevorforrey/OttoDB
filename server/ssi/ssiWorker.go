package ssi

import (
	"OttoDB/server/ssi/ssiLockTable"
	"fmt"
)

func ManageSSITable(keyLockTable *ssiLockTable.SIReadKeyLockTable, newTxns chan uint64, endingTxns chan uint64) {
	txnDetailsTable := ssiLockTable.NewSIReadTxnDetailsTable()
	for {
		select {
		case newTxnID := <-newTxns:
			fmt.Println("received new transaction", newTxnID)
			for _, details := range txnDetailsTable.Details {
				if details.Status == ssiLockTable.InProgress {
					details.BlockingTxns = append(details.BlockingTxns, newTxnID)
				}
			}
		case endedTxnID := <-endingTxns:
			fmt.Println("received ending transaction", endedTxnID)

			// var txnSILocksToRelease []uint64
			txnSILocksToRelease := make(map[uint64]bool)

			// Set ending txn as ended (committed) in the txn details table
			endingTxnDetails := txnDetailsTable.Details[endedTxnID]
			endingTxnDetails.Status = ssiLockTable.Committed
			txnDetailsTable.Details[endedTxnID] = endingTxnDetails

			// For every transaction in the SI lock table,
			// if the ended txn exists in their waiting txns, remove it.
			// if removing results in an empty list of waiting txns, tag it
			// for deletion in the key lock table
			for _, details := range txnDetailsTable.Details {
				for index, holdingTxn := range details.BlockingTxns {
					if holdingTxn == endedTxnID {
						details.BlockingTxns = append(details.BlockingTxns[:index], details.BlockingTxns[index+1:]...)
						if len(details.BlockingTxns) == 0 {
							txnSILocksToRelease[holdingTxn] = true
						}
					}
				}
			}

			// Remove Txns From Key Table (if they no longer need to hold SIREAD locks)
			keyLockTable.Lock()
			for _, holdingTxns := range keyLockTable.Table {
				for index, holdingTxn := range holdingTxns {
					if txnSILocksToRelease[holdingTxn] {
						holdingTxns = append(holdingTxns[:index], holdingTxns[index+1:]...)
					}
				}
			}
			keyLockTable.Unlock()

		default:
		}
	}
}
