package binTree

import (
	"OttoDB/server/store/record"
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
	records []record.Record
}

type node struct {
	data   recordList
	left   *node
	right  *node
	parent *node
}

type BinTree struct {
	sync.RWMutex
	root *node
}

func NewTree() *BinTree {
	tree := BinTree{}
	return &tree
}

func (tree *BinTree) Get(key string, timestamp uint64, activeTxns map[uint64]bool) (string, error) {
	fmt.Printf("About to start tree search on %s\n", key)
	getNode := tree.Search(tree.root, key)
	if getNode == nil {
		return "", errors.New("No value found")
	}

	recordList := getNode.data.records
	fmt.Printf("Found key: %s\n", getNode.data.key)

	// Find value scoped in current timestamp that's committed
	var returnValue string
	var nonVisibleRecords []uint64
	for i := len(recordList) - 1; i >= 0; i-- {
		currRecord := recordList[i]
		if currRecord.IsVisible(timestamp, activeTxns) {
			returnValue = currRecord.Value
			break
		} else {
			// Won't work all the time, could be the case where it's deleted, but doesn't cause a rw antidep
			nonVisibleRecords = append(nonVisibleRecords, currRecord.CreatedBy)
		}
	}

	// If return value isn't string zero value, return proper value
	if returnValue != "" {
		fmt.Printf("Going to send value: %s\n", returnValue)
		return returnValue, nil
	}
	return "", errors.New("No value for provided timestamp")
}

func (tree *BinTree) Set(key string, value string, timestamp uint64, activeTxns map[uint64]bool) (*record.Record, error) {

	var newRecord = record.NewRecord(value, timestamp)
	var singleRecordList = recordList{key: key, records: []record.Record{newRecord}}

	insertedRecord, err := tree.insert(key, singleRecordList, timestamp, activeTxns)
	if err != nil {
		return nil, err
	}
	return insertedRecord, nil
}

func (tree *BinTree) Search(root *node, key string) *node {
	if root == nil || key == root.data.key {
		return root
	}
	if key < tree.root.data.key {
		fmt.Println("key is less than root key")
		return tree.Search(root.left, key)
	} else {
		fmt.Println("key is greater than root key")
		return tree.Search(root.right, key)
	}
}

func (tree *BinTree) insert(key string, singleRecordList recordList, timestamp uint64, activeTxns map[uint64]bool) (*record.Record, error) {
	newNode := node{}
	newNode.data = singleRecordList
	var insertedRecord *record.Record

	if tree.root == nil {
		tree.root = &newNode
		insertedRecord = &newNode.data.records[0]
	} else {
		fmt.Println("calling to insert node")
		var err error
		insertedRecord, err = tree.iterativeInsert(tree.root, &newNode, timestamp, activeTxns)
		if err != nil {
			return nil, err
		}
	}
	return insertedRecord, nil
}

func (tree *BinTree) iterativeInsert(root *node, newNode *node, timestamp uint64, activeTxns map[uint64]bool) (*record.Record, error) {
	for {
		if newNode.data.key < root.data.key {
			if root.left == nil {
				root.left = newNode
				return &newNode.data.records[0], nil
			}
			root = root.left

		} else if newNode.data.key > root.data.key {
			if root.right == nil {
				root.right = newNode
				return &newNode.data.records[0], nil
			}
			root = root.right

		} else {
			// Adding a version to an existing key
			root.data.Lock()
			defer root.data.Unlock()

			// Check for concurrent write
			lastRecord := root.data.records[len(root.data.records)-1]

			if isAlreadyEdited, error := lastRecord.IsConcurrentEdited(timestamp, activeTxns); isAlreadyEdited {
				return nil, error
			}

			root.data.records = append(root.data.records, newNode.data.records[0])
			fmt.Println("new node inserted")
			return &root.data.records[len(root.data.records)-1], nil
		}
	}
}

func (tree *BinTree) getMinimum(currNode *node) *node {
	for currNode.left != nil {
		currNode = currNode.left
	}
	return currNode
}

func (tree *BinTree) Expire(key string, timestamp uint64, activeTxns map[uint64]bool) (*record.Record, error) {
	delNode := tree.Search(tree.root, key)
	if delNode != nil {
		delNode.data.Lock()
		defer delNode.data.Unlock()

		recordLen := len(delNode.data.records)
		delRecord := &delNode.data.records[recordLen-1]

		if isAlreadyEdited, error := delRecord.IsConcurrentEdited(timestamp, activeTxns); isAlreadyEdited {
			return nil, error
		}

		delRecord.OldExpiredBy = delRecord.ExpiredBy
		delRecord.ExpiredBy = timestamp
		return &delNode.data.records[recordLen-1], nil
	}
	return nil, nil
}

// Expire with active txns ignored (used for replaying log)
func (tree *BinTree) ExpireReplay(key string, timestamp uint64) (*record.Record, error) {
	delNode := tree.Search(tree.root, key)
	if delNode != nil {
		recordLen := len(delNode.data.records)
		delRecord := &delNode.data.records[recordLen-1]

		delRecord.OldExpiredBy = delRecord.ExpiredBy
		delRecord.ExpiredBy = timestamp
		return &delNode.data.records[recordLen-1], nil
	}
	return nil, nil
}

func (tree *BinTree) BreadthFirstTraversal() {
	if tree.root == nil {
		return
	}
	nodes := make([]node, 1)
	nodes[0] = *tree.root
	for len(nodes) != 0 {
		currentNode := nodes[0]
		if currentNode.data.key == (*tree).root.data.key {
			fmt.Printf("%s \n", currentNode.data.key)
		} else {
			fmt.Printf("%s -> %s\n", currentNode.parent.data.key, currentNode.data.key)
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
	tree.inOrderTraversal(tree.root)
}

func (tree *BinTree) inOrderTraversal(currNode *node) {
	if currNode == nil {
		return
	}
	tree.inOrderTraversal(currNode.left)
	fmt.Printf("%s: \n", currNode.data.key)
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
	startingNode.node = *(tree.root)
	nodes := make([]boundedNode, 1)
	nodes = append(nodes, startingNode)
	for len(nodes) != 0 {
		var nodeAndBound boundedNode
		// Pop from nodes stack
		nodeAndBound, nodes = nodes[0], nodes[1:]
		currNode := nodeAndBound.node
		currNodeKey := currNode.data.key
		lowerBound := nodeAndBound.lowerBound
		upperBound := nodeAndBound.upperBound

		// Check to see if key is in the proper upper / lower bound
		if (currNodeKey <= lowerBound || currNodeKey >= upperBound) && (currNode.data.key != "" && upperBound != "" && lowerBound != "") {
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
	nodeToPrint := tree.Search(tree.root, key)
	var sb strings.Builder
	recordList := nodeToPrint.data.records
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

	var newRecord = record.NewRecord(value, timestamp)
	var singleRecordList = recordList{key: key, records: []record.Record{newRecord}}

	insertedRecord, err := tree.insertReplay(key, singleRecordList, timestamp)
	if err != nil {
		return nil, err
	}
	return insertedRecord, nil
}

func (tree *BinTree) insertReplay(key string, singleRecordList recordList, timestamp uint64) (*record.Record, error) {
	newNode := node{}
	newNode.data = singleRecordList
	var insertedRecord *record.Record

	if tree.root == nil {
		tree.root = &newNode
		insertedRecord = &newNode.data.records[0]
	} else {
		fmt.Println("calling to insert node")
		var err error
		insertedRecord, err = tree.iterativeInsertReplay(tree.root, &newNode, timestamp)
		if err != nil {
			return nil, err
		}
	}
	return insertedRecord, nil
}

func (tree *BinTree) iterativeInsertReplay(root *node, newNode *node, timestamp uint64) (*record.Record, error) {
	for {
		if newNode.data.key < root.data.key {
			if root.left == nil {
				root.left = newNode
				return &newNode.data.records[0], nil
			}
			root = root.left

		} else if newNode.data.key > root.data.key {
			if root.right == nil {
				root.right = newNode
				return &newNode.data.records[0], nil
			}
			root = root.right

		} else {
			root.data.records = append(root.data.records, newNode.data.records[0])
			fmt.Println("new node inserted")
			return &newNode.data.records[0], nil
		}
	}
}
