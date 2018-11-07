package binTree

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
)

type record struct {
	value     string
	timestamp uint64
	expired   uint64
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
		// TODO account for expiration
		if timestamp >= currRecord.timestamp && (!activeTxns[currRecord.timestamp] || (activeTxns[currRecord.timestamp] && currRecord.timestamp == timestamp)) {
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

	var newRecord = record{value: value, timestamp: timestamp, expired: 0}
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
			root.data.Lock()
			defer root.data.Unlock()

			// Check for concurrent write
			lastRecord := root.data.records[len(root.data.records)-1]
			if lastRecord.timestamp > timestamp {
				return errors.New("A committed transaction wrote to this key")
				//TODO check for node that should be deleted
				//TODO decide if this should still error
				// Is a long transaction writing over a quicker txn bad practice?
			} else if activeTxns[lastRecord.timestamp] && lastRecord.timestamp != timestamp {
				return errors.New("An active transaction wrote to this key")
			}

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
		delNode.data.records[recordLen-1].expired = timestamp
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

		sb.WriteString("timestamp: ")
		sb.WriteString(strconv.Itoa(int(record.timestamp)))
		sb.WriteString("   |")

		sb.WriteString("expiration: ")
		sb.WriteString(strconv.Itoa(int(record.expired)))
		sb.WriteString("   |")
		sb.WriteString("\n")
	}
	return sb.String()
}
