package binTree

import (
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

type record struct {
	value     string
	createdBy uint64
	expiredBy uint64
	status    txnStatus
}

type recordList struct {
	sync.RWMutex
	key     string
	records []record
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
		return "no value found", errors.New("No value found")
	}
	// if getNode.data.expired != 0 && getNode.data.expired <= timestamp && !activeTxns[getNode.data.expired] {
	// 	return "the node has been deleted", nil
	// }
	recordList := getNode.data.records
	fmt.Printf("Found key: %s\n", getNode.data.key)

	// Find value scoped in current timestamp that's committed
	var returnValue string
	for i := len(recordList) - 1; i >= 0; i-- {
		currRecord := recordList[i]
		if currRecord.isVisible(timestamp, activeTxns) {
			returnValue = currRecord.value
			break
		}
	}

	// If return value isn't string zero value, return proper value
	if returnValue != "" {
		fmt.Printf("Going to send value: %s\n", returnValue)
		return returnValue, nil
	}
	return "no value found", errors.New("No value for provided timestamp")
}

func (tree *BinTree) Set(key string, value string, timestamp uint64, activeTxns map[uint64]bool) error {

	var newRecord = record{value: value, createdBy: timestamp, expiredBy: 0}
	var singleRecordList = recordList{key: key, records: []record{newRecord}}

	err := tree.insert(key, singleRecordList, timestamp, activeTxns)
	if err != nil {
		return err
	}
	return nil
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

func (tree *BinTree) insert(key string, singleRecordList recordList, timestamp uint64, activeTxns map[uint64]bool) error {
	newNode := node{}
	newNode.data = singleRecordList

	if tree.root == nil {
		tree.root = &newNode
	} else {
		fmt.Println("calling to insert node")
		err := tree.iterativeInsert(tree.root, &newNode, timestamp, activeTxns)
		if err != nil {
			return err
		}
	}
	return nil
}

func (tree *BinTree) iterativeInsert(root *node, newNode *node, timestamp uint64, activeTxns map[uint64]bool) error {
	for {
		if newNode.data.key < root.data.key {
			if root.left == nil {
				root.left = newNode
				return nil
			}
			root = root.left

		} else if newNode.data.key > root.data.key {
			if root.right == nil {
				root.right = newNode
				return nil
			}
			root = root.right

		} else {
			// Adding a version to an existing key
			root.data.Lock()
			defer root.data.Unlock()

			// Check for concurrent write
			root.data.records[len(root.data.records)-1].expiredBy = timestamp

			// lastRecord.expiredBy = timestamp
			// if lastRecord.createdBy > timestamp {
			// 	return errors.New("A committed transaction wrote to this key")

			// } else if activeTxns[lastRecord.createdBy] && lastRecord.createdBy != timestamp {
			// 	return errors.New("An active transaction wrote to this key")
			// }

			root.data.records = append(root.data.records, newNode.data.records[0])
			fmt.Println("new node inserted")
			return nil
		}
	}
}

func (tree *BinTree) getMinimum(currNode *node) *node {
	for currNode.left != nil {
		currNode = currNode.left
	}
	return currNode
}

func (tree *BinTree) Expire(key string, timestamp uint64, activeTxns map[uint64]bool) error {
	delNode := tree.Search(tree.root, key)
	if delNode != nil {
		delNode.data.Lock()
		defer delNode.data.Unlock()
		recordLen := len(delNode.data.records)
		delNode.data.records[recordLen-1].expiredBy = timestamp
		return nil
	}
	return errors.New("could not expire node that didnt exist")
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

func (currRecord *record) isVisible(txnID uint64, activeTxns map[uint64]bool) bool {
	// We can't view results from transactions that started before us
	if currRecord.createdBy > txnID {
		return false
	}
	// We can't view a record if it's in an active transaction that isn't our own
	if activeTxns[currRecord.createdBy] && currRecord.createdBy != txnID {
		return false
	}
	// We can't view a record if
	// - it's expired and not active
	// - it's expired and the transaction iD is our own
	if currRecord.expiredBy != 0 && (!activeTxns[currRecord.expiredBy] || currRecord.expiredBy == txnID) {
		return false
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
		sb.WriteString(record.value)
		sb.WriteString("   |")

		sb.WriteString("created: ")
		sb.WriteString(strconv.Itoa(int(record.createdBy)))
		sb.WriteString("   |")

		sb.WriteString("expired: ")
		sb.WriteString(strconv.Itoa(int(record.expiredBy)))
		sb.WriteString("   |")
		sb.WriteString("\n")
	}
	return sb.String()
}
