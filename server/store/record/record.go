package record

import (
	"errors"
)

type txnStatus int

const (
	InProgress txnStatus = iota
	Aborted
	Committed
)

type Record struct {
	Value        string
	CreatedBy    uint64
	ExpiredBy    uint64
	OldExpiredBy uint64
	Status       txnStatus
}

func NewRecord(value string, createdBy uint64) Record {
	return Record{Value: value, CreatedBy: createdBy}
}

func (currRecord *Record) IsVisible(txnID uint64, activeTxns map[uint64]bool) bool {
	// We can't view a record if its been aborted
	if currRecord.Status == Aborted {
		return false
	}

	// We can't view results from transactions that started before us
	if currRecord.CreatedBy > txnID {
		return false
	}
	// We can't view a record if it's in an active transaction that isn't our own
	if activeTxns[currRecord.CreatedBy] && currRecord.CreatedBy != txnID {
		return false
	}
	// We can't view a record if
	// - it's expired and not active
	// - it's expired and the transaction iD is our own
	if currRecord.ExpiredBy != 0 && (!activeTxns[currRecord.ExpiredBy] || currRecord.ExpiredBy == txnID) {
		return false
	}
	return true
}

func (lastRecord *Record) IsConcurrentEdited(txnID uint64, activeTxns map[uint64]bool) (bool, error) {
	// Catches all committed and noncommitted future transaction writes
	if lastRecord.CreatedBy > txnID {
		return true, errors.New("A later transaction wrote/is writing to this key")
	} else if activeTxns[lastRecord.CreatedBy] && lastRecord.CreatedBy != txnID {
		// Catches all uncommitted previous transaction writes
		return true, errors.New("An active transaction wrote to this key")
	}

	if lastRecord.ExpiredBy > txnID {
		return true, errors.New("A later transaction deleted/is deleting this key")
	} else if activeTxns[lastRecord.ExpiredBy] && lastRecord.ExpiredBy != txnID {
		return true, errors.New("An active transaction is deleting this key")
	}

	return false, nil
}
