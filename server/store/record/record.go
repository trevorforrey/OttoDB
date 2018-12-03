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
	Key          string
	Value        string
	CreatedBy    uint64
	ExpiredBy    uint64
	OldExpiredBy uint64
	Status       txnStatus
}

func NewRecord(key string, value string, createdBy uint64) Record {
	return Record{Key: key, Value: value, CreatedBy: createdBy}
}

func (currRecord *Record) IsVisible(txnID uint64, activeTxns map[uint64]bool) (visibility bool, rwAntiDep bool) {
	// We can't view a record if its been aborted
	if currRecord.Status == Aborted {
		return false, false
	}

	// We can't view results from transactions that started after us
	if currRecord.CreatedBy > txnID {
		return false, true
	}
	// We can't view a record if it's in an active transaction that isn't our own
	if activeTxns[currRecord.CreatedBy] && currRecord.CreatedBy != txnID {
		return false, true
	}

	// We can't view a record if
	// - it's expired and not active
	// - it's expired and the transaction iD is our own
	if currRecord.ExpiredBy != 0 {
		if !activeTxns[currRecord.ExpiredBy] {
			return false, false
		}
		if currRecord.ExpiredBy == txnID {
			return false, false
		}
		// If expiring transaction is active and not our own, we can read, but a rw-antidep is formed
		return true, true
	}
	return true, false
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
