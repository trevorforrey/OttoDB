package binTree

import (
	"OttoDB/server/ssi/ssiLockTable"
	"OttoDB/server/store/record"
	"OttoDB/server/transaction"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

type txnStatus int

const (
	InProgress txnStatus = iota
	Aborted
	Committed
)

type recordList struct {
	sync.RWMutex
	key     string
	Records []record.Record
}

type node struct {
	Data   recordList
	left   *node
	right  *node
	parent *node
}

type BinTree struct {
	sync.RWMutex
	Root *node
}

func NewTree() *BinTree {
	tree := BinTree{}
	return &tree
}

func (tree *BinTree) Get(key string, timestamp uint64, activeTxns map[uint64]bool, txnMap *transaction.TransactionMap, siReadLockTable *ssiLockTable.SIReadKeyLockTable) (string, error) {
	fmt.Printf("About to start tree search on %s\n", key)
	getNode := tree.Search(tree.Root, key)
	if getNode == nil {
		return "", errors.New("No value found")
	}

	recordList := getNode.Data.Records
	fmt.Printf("Found key: %s\n", getNode.Data.key)

	// Find value scoped in current timestamp that's committed
	var returnValue string
	for i := len(recordList) - 1; i >= 0; i-- {
		currRecord := recordList[i]
		if isVisible, isRWAntiDep := currRecord.IsVisible(timestamp, activeTxns); isVisible {
			returnValue = currRecord.Value

			// Add SI Read Lock on the key to SI Read Lock Table
			if txnMap.Transactions[timestamp] != nil {
				siReadLockTable.Lock()
				locksOnKey := siReadLockTable.Table[key]
				locksOnKey = append(locksOnKey, timestamp)
				siReadLockTable.Table[key] = locksOnKey
				siReadLockTable.Unlock()
			}

			// if reading an active delete (RW-AntiDep)
			if isRWAntiDep && txnMap.Transactions[timestamp] != nil {
				fmt.Printf("(expired) RW Dep out: %d", currRecord.ExpiredBy)
				txnMap.Lock()
				err := txnMap.AddRWAntiDepFlagOut(timestamp, currRecord.ExpiredBy)
				txnMap.Unlock()
				if err != nil {
					return "", err
				}
			}
			break
		} else if isRWAntiDep && txnMap.Transactions[timestamp] != nil {
			fmt.Printf("(write) RW Dep out: %d", currRecord.CreatedBy)
			txnMap.Lock()
			err := txnMap.AddRWAntiDepFlagOut(timestamp, currRecord.CreatedBy)
			txnMap.Unlock()
			if err != nil {
				return "", err
			}
		}
	}

	// If return value isn't string zero value, return proper value
	if returnValue != "" {
		fmt.Printf("Going to send value: %s\n", returnValue)
		return returnValue, nil
	}
	return "", errors.New("No value for provided timestamp")
}

func (tree *BinTree) Set(key string, value string, timestamp uint64, activeTxns map[uint64]bool, txnMap *transaction.TransactionMap, siReadLockTable *ssiLockTable.SIReadKeyLockTable) (*record.Record, error) {

	var newRecord = record.NewRecord(key, value, timestamp)
	var singleRecordList = recordList{key: key, Records: []record.Record{newRecord}}

	insertedRecord, err := tree.insert(key, singleRecordList, timestamp, activeTxns, txnMap, siReadLockTable)
	if err != nil {
		return nil, err
	}
	return insertedRecord, nil
}

func (tree *BinTree) Search(root *node, key string) *node {
	if root == nil || key == root.Data.key {
		return root
	}
	if key < root.Data.key {
		fmt.Println("key is less than root key")
		return tree.Search(root.left, key)
	} else {
		fmt.Println("key is greater than root key")
		return tree.Search(root.right, key)
	}
}

func (tree *BinTree) insert(key string, singleRecordList recordList, timestamp uint64, activeTxns map[uint64]bool, txnMap *transaction.TransactionMap, siReadLockTable *ssiLockTable.SIReadKeyLockTable) (*record.Record, error) {
	newNode := node{}
	newNode.Data = singleRecordList
	var insertedRecord *record.Record

	if tree.Root == nil {
		tree.Root = &newNode
		insertedRecord = &tree.Root.Data.Records[0]
	} else {
		fmt.Println("calling to insert node")
		var err error
		insertedRecord, err = tree.iterativeInsert(tree.Root, &newNode, timestamp, activeTxns, txnMap, siReadLockTable)
		if err != nil {
			return nil, err
		}
	}
	return insertedRecord, nil
}

func (tree *BinTree) iterativeInsert(root *node, newNode *node, timestamp uint64, activeTxns map[uint64]bool, txnMap *transaction.TransactionMap, siReadLockTable *ssiLockTable.SIReadKeyLockTable) (*record.Record, error) {
	for {
		if newNode.Data.key < root.Data.key {
			if root.left == nil {
				root.left = newNode
				return &root.left.Data.Records[0], nil
			}
			root = root.left

		} else if newNode.Data.key > root.Data.key {
			if root.right == nil {
				root.right = newNode
				return &root.right.Data.Records[0], nil
			}
			root = root.right

		} else {
			// Adding a version to an existing key
			root.Data.Lock()
			defer root.Data.Unlock()

			// Check for concurrent write
			lastRecord := root.Data.Records[len(root.Data.Records)-1]

			if isAlreadyEdited, error := lastRecord.IsConcurrentEdited(timestamp, activeTxns); isAlreadyEdited {
				return nil, error
			}

			siReadLockTable.RLock()
			txnsHoldingSIReadLockOnKey := siReadLockTable.Table[newNode.Data.key]
			siReadLockTable.RUnlock()

			// If there are txns holding an SIREAD lock on the key
			if len(txnsHoldingSIReadLockOnKey) != 0 {
				// Add RW AntiDependencies to the acting txns
				txnMap.Lock()
				for _, txn := range txnsHoldingSIReadLockOnKey {
					err := txnMap.AddRWAntiDepFlagIn(txn, timestamp)
					if err != nil {
						txnMap.Unlock()
						return nil, err
					}
				}
				txnMap.Unlock()
			}

			fmt.Printf("\n%p", &root.Data.Records[0])
			fmt.Printf("Capacity of record list is: %d", cap(root.Data.Records))
			root.Data.Records = append(root.Data.Records, newNode.Data.Records[0]) // The culprit
			fmt.Printf("\n%p", &root.Data.Records[0])
			fmt.Printf("\n%p", &root.Data.Records[1])
			fmt.Println("new version inserted")
			return &root.Data.Records[len(root.Data.Records)-1], nil
		}
	}
}

func (tree *BinTree) getMinimum(currNode *node) *node {
	for currNode.left != nil {
		currNode = currNode.left
	}
	return currNode
}

func (tree *BinTree) Expire(key string, timestamp uint64, activeTxns map[uint64]bool, txnMap *transaction.TransactionMap, siReadLockTable *ssiLockTable.SIReadKeyLockTable) (*record.Record, error) {
	delNode := tree.Search(tree.Root, key)
	if delNode != nil {
		delNode.Data.Lock()
		defer delNode.Data.Unlock()

		recordLen := len(delNode.Data.Records)
		delRecord := &delNode.Data.Records[recordLen-1]

		if isAlreadyEdited, error := delRecord.IsConcurrentEdited(timestamp, activeTxns); isAlreadyEdited {
			return nil, error
		}

		siReadLockTable.RLock()
		txnsHoldingSIReadLockOnKey := siReadLockTable.Table[key]
		siReadLockTable.RUnlock()

		// If there are txns holding an SIREAD lock on the key
		if len(txnsHoldingSIReadLockOnKey) != 0 {
			// Add RW AntiDependencies to the acting txns
			txnMap.Lock()
			for _, txn := range txnsHoldingSIReadLockOnKey {
				err := txnMap.AddRWAntiDepFlagIn(txn, timestamp)
				if err != nil {
					txnMap.Unlock()
					return nil, err
				}
			}
			txnMap.Unlock()
		}

		delRecord.OldExpiredBy = delRecord.ExpiredBy
		delRecord.ExpiredBy = timestamp
		return &delNode.Data.Records[recordLen-1], nil
	}
	return nil, nil
}

// Expire with active txns ignored (used for replaying log)
func (tree *BinTree) ExpireReplay(key string, timestamp uint64) (*record.Record, error) {
	delNode := tree.Search(tree.Root, key)
	if delNode != nil {
		recordLen := len(delNode.Data.Records)
		delRecord := &delNode.Data.Records[recordLen-1]

		delRecord.OldExpiredBy = delRecord.ExpiredBy
		delRecord.ExpiredBy = timestamp
		return &delNode.Data.Records[recordLen-1], nil
	}
	return nil, nil
}

func (tree *BinTree) BreadthFirstTraversal() {
	if tree.Root == nil {
		return
	}
	nodes := make([]node, 1)
	nodes[0] = *tree.Root
	for len(nodes) != 0 {
		currentNode := nodes[0]
		if currentNode.Data.key == (*tree).Root.Data.key {
			fmt.Printf("%s \n", currentNode.Data.key)
		} else {
			fmt.Printf("%s -> %s\n", currentNode.parent.Data.key, currentNode.Data.key)
		}
		// Remove current element from the slice
		nodes = append(nodes[:0], nodes[1:]...)
		// Append nodes children, if non-nil
		if currentNode.left != nil {
			nodes = append(nodes, *currentNode.left)
		}
		if currentNode.right != nil {
			nodes = append(nodes, *currentNode.right)
		}
	}

}

func (tree *BinTree) InOrderTraversal() {
	tree.inOrderTraversal(tree.Root)
}

func (tree *BinTree) inOrderTraversal(currNode *node) {
	if currNode == nil {
		return
	}
	tree.inOrderTraversal(currNode.left)
	fmt.Printf("%s: \n", currNode.Data.key)
	tree.inOrderTraversal(currNode.right)
}

// Returns true if tree is sorted properly
func (tree *BinTree) Sorted() bool {
	type boundedNode struct {
		node       node
		lowerBound string
		upperBound string
	}
	startingNode := boundedNode{}
	startingNode.node = *(tree.Root)
	nodes := make([]boundedNode, 1)
	nodes = append(nodes, startingNode)
	for len(nodes) != 0 {
		var nodeAndBound boundedNode
		// Pop from nodes stack
		nodeAndBound, nodes = nodes[0], nodes[1:]
		currNode := nodeAndBound.node
		currNodeKey := currNode.Data.key
		lowerBound := nodeAndBound.lowerBound
		upperBound := nodeAndBound.upperBound

		// Check to see if key is in the proper upper / lower bound
		if (currNodeKey <= lowerBound || currNodeKey >= upperBound) && (currNode.Data.key != "" && upperBound != "" && lowerBound != "") {
			fmt.Printf("Key is %s. Lower bound is %s. Upper Bound is %s\n", currNodeKey, lowerBound, upperBound)
			return false
		} else {
			fmt.Printf("Lower : (Key) : Upper - %s : (%s) : %s\n", lowerBound, currNodeKey, upperBound)
		}

		// push any left / right children of current node
		if currNode.left != nil {
			var leftNodeAndBound boundedNode
			leftNodeAndBound.node = *(currNode.left)
			leftNodeAndBound.lowerBound = lowerBound
			leftNodeAndBound.upperBound = currNodeKey
			nodes = append(nodes, leftNodeAndBound)
		}
		if currNode.right != nil {
			var rightNodeAndBound boundedNode
			rightNodeAndBound.node = *(currNode.right)
			rightNodeAndBound.lowerBound = currNodeKey
			rightNodeAndBound.upperBound = upperBound
			nodes = append(nodes, rightNodeAndBound)
		}

	}
	return true
}

func (tree *BinTree) RecordListPrint(key string) string {
	nodeToPrint := tree.Search(tree.Root, key)
	var sb strings.Builder
	recordList := nodeToPrint.Data.Records
	for index, record := range recordList {
		sb.WriteString("index: ")
		fmt.Printf("index: %d", index)
		sb.WriteString(strconv.Itoa(int(index)))
		sb.WriteString("   |")

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

func (tree *BinTree) SetReplay(key string, value string, timestamp uint64) (*record.Record, error) {

	var newRecord = record.NewRecord(key, value, timestamp)
	var singleRecordList = recordList{key: key, Records: []record.Record{newRecord}}

	insertedRecord, err := tree.insertReplay(key, singleRecordList, timestamp)
	if err != nil {
		return nil, err
	}
	return insertedRecord, nil
}

func (tree *BinTree) insertReplay(key string, singleRecordList recordList, timestamp uint64) (*record.Record, error) {
	newNode := node{}
	newNode.Data = singleRecordList
	var insertedRecord *record.Record

	if tree.Root == nil {
		tree.Root = &newNode
		insertedRecord = &newNode.Data.Records[0]
	} else {
		fmt.Println("calling to insert node")
		var err error
		insertedRecord, err = tree.iterativeInsertReplay(tree.Root, &newNode, timestamp)
		if err != nil {
			return nil, err
		}
	}
	return insertedRecord, nil
}

func (tree *BinTree) iterativeInsertReplay(root *node, newNode *node, timestamp uint64) (*record.Record, error) {
	for {
		if newNode.Data.key < root.Data.key {
			if root.left == nil {
				root.left = newNode
				return &newNode.Data.Records[0], nil
			}
			root = root.left

		} else if newNode.Data.key > root.Data.key {
			if root.right == nil {
				root.right = newNode
				return &newNode.Data.Records[0], nil
			}
			root = root.right

		} else {
			root.Data.Records = append(root.Data.Records, newNode.Data.Records[0])
			fmt.Println("new node inserted")
			return &newNode.Data.Records[0], nil
		}
	}
}

func (tree *BinTree) Abort(txn *transaction.Transaction) error {
	err := tree.resetInsertedRecords(txn.Timestamp, txn.InsertedRecords)
	if err != nil {
		return err
	}
	err = tree.resetDeletedRecords(txn.Timestamp, txn.DeletedRecords)
	if err != nil {
		return err
	}
	return nil
}

func (tree *BinTree) resetInsertedRecords(timestamp uint64, insertedRecords []*record.Record) error {
	for _, insertedRecord := range insertedRecords {
		err := tree.resetInsertedRecord(timestamp, insertedRecord)
		if err != nil {
			return err
		}
	}
	return nil
}

func (tree *BinTree) resetInsertedRecord(timestamp uint64, insertedRecord *record.Record) error {
	node := tree.Search(tree.Root, insertedRecord.Key)
	if len(node.Data.Records) == 0 {
		return errors.New("No record found for aborting inserted record")
	}
	node.Data.Lock()
	defer node.Data.Unlock()
	for index, rec := range node.Data.Records {
		if rec.CreatedBy == timestamp {
			node.Data.Records[index].Status = record.Aborted
		}
	}
	return nil
}

func (tree *BinTree) resetDeletedRecords(timestamp uint64, deletedRecords []*record.Record) error {
	for _, deletedRecord := range deletedRecords {
		err := tree.resetDeletedRecord(timestamp, deletedRecord)
		if err != nil {
			return err
		}
	}
	return nil
}

func (tree *BinTree) resetDeletedRecord(timestamp uint64, deletedRecord *record.Record) error {
	node := tree.Search(tree.Root, deletedRecord.Key)
	if len(node.Data.Records) == 0 {
		return errors.New("No records to delete")
	}
	node.Data.Lock()
	defer node.Data.Unlock()
	for index, rec := range node.Data.Records {
		if rec.ExpiredBy == timestamp {
			node.Data.Records[index].ExpiredBy = node.Data.Records[index].OldExpiredBy
		}
	}
	return nil
}
