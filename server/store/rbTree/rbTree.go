package rbTree

import (
	"errors"
	"fmt"
	"sync"
)

type record struct {
	value     string
	timestamp uint64
}

type recordList struct {
	key     string
	records []record
	expired uint64
}

type nodeColor int

const (
	Red nodeColor = iota
	Black
)

type node struct {
	color  nodeColor
	data   recordList
	left   *node
	right  *node
	parent *node
}

type RBTree struct {
	sync.RWMutex
	root *node
}

func NewTree() *RBTree {
	tree := RBTree{}
	return &tree
}

func (tree *RBTree) Get(key string, timestamp uint64, activeTxns map[uint64]bool) (string, error) {
	tree.RLock()
	defer tree.RUnlock()
	fmt.Printf("About to start tree search on %s\n", key)
	getNode := tree.Search(tree.root, key)
	if getNode == nil {
		return "no value found", errors.New("No value found")
	}
	if getNode.data.expired != 0 && getNode.data.expired <= timestamp && !activeTxns[getNode.data.expired] {
		return "the node has been deleted", nil
	}
	recordList := getNode.data.records
	fmt.Printf("Found key: %s\n", getNode.data.key)

	// Find value scoped in current timestamp that's committed
	var returnValue string
	for i := len(recordList) - 1; i >= 0; i-- {
		currRecord := recordList[i]
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

// TODO If this set becomes more of an update, then it should set the expiration
// date of the previous value to the current timestamp

// Need to move expiration from the recordList to the individual records (page 240)
func (tree *RBTree) Set(key string, value string, timestamp uint64, activeTxns map[uint64]bool) error {
	tree.Lock()
	defer tree.Unlock()
	var newRecord = record{value: value, timestamp: timestamp}
	var singleRecordList = recordList{key: key, records: []record{newRecord}}

	// Check to see if another transaction that has completed has written to the node
	// Setting on current txn is not valid if
	// 	- an active transaction wrote to the key already - (retry)
	//	- a txid greater than mine wrote to the key already - (abort)
	nodeToSet := tree.Search(tree.root, key)

	// If Set is truly just an update
	if nodeToSet != nil {
		lastRecord := nodeToSet.data.records[len(nodeToSet.data.records)-1]
		if lastRecord.timestamp > timestamp {
			return errors.New("A committed transaction wrote to this key")
		} else if activeTxns[lastRecord.timestamp] && lastRecord.timestamp != timestamp {
			return errors.New("An active transaction wrote to this key")
		}
		nodeToSet.data.expired = 0
		nodeToSet.data.records = append(nodeToSet.data.records, newRecord)
		return nil
	}

	// If Set needs to insert a new node
	tree.insert(key, singleRecordList)
	return nil
}

func (tree *RBTree) Search(root *node, key string) *node {
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

func (tree *RBTree) insert(key string, singleRecordList recordList) {
	newNode := node{}
	newNode.data = singleRecordList
	newNode.color = Red

	if tree.root == nil {
		newNode.color = Black
		tree.root = &newNode
	} else {
		fmt.Println("calling to insert node")
		insertedNode := tree.insertHelper(tree.root, &newNode)
		insertedNode.data.expired = 0
		tree.fixViolation(&newNode)
	}
}

func (tree *RBTree) insertHelper(root *node, newNode *node) *node {
	if root == nil {
		fmt.Println("new node inserted")
		return newNode
	}

	if newNode.data.key < root.data.key {
		// fmt.Println("new node is less than root key")
		root.left = tree.insertHelper(root.left, newNode)
		root.left.parent = root
	} else if newNode.data.key > root.data.key {
		// fmt.Println("new node key greater than root key")
		root.right = tree.insertHelper(root.right, newNode)
		root.right.parent = root
	} else {
		// root.data.records[len(root.data.records)-1].expiration = timestamp
		root.data.records = append(root.data.records, newNode.data.records[0])
		fmt.Println("new node inserted")
	}

	return root
}

func (tree *RBTree) fixViolation(newNode *node) {
	fmt.Println("Starting to fix the violation")
	for newNode.parent != nil && newNode.parent.color == Red {
		// fmt.Println("In loop")
		if newNode.parent == newNode.parent.parent.left {
			// fmt.Println("parent is left child")
			uncle := newNode.parent.parent.right
			if uncle != nil && uncle.color == Red {
				// fmt.Println("uncle is red")
				newNode.parent.color = Black
				uncle.color = Black
				newNode.parent.parent.color = Red
				newNode = newNode.parent.parent
			} else {
				// fmt.Println("uncle is black")
				if newNode == newNode.parent.right {
					// fmt.Println("new node is a left child")
					newNode = newNode.parent
					tree.leftRotate(newNode)
				}
				// fmt.Println("new node is a right child")
				newNode.parent.color = Black
				newNode.parent.parent.color = Red
				tree.rightRotate(newNode.parent.parent)
			}
		} else {
			// fmt.Println("parent is right child")
			uncle := newNode.parent.parent.left
			if uncle != nil && uncle.color == Red {
				// fmt.Println("uncle is red")
				newNode.parent.color = Black
				uncle.color = Black
				newNode.parent.parent.color = Red
				newNode = newNode.parent.parent
			} else {
				// fmt.Println("uncle is black")
				if newNode == newNode.parent.left {
					// fmt.Println("new node is a left child")
					newNode = newNode.parent
					tree.rightRotate(newNode)
				}
				// fmt.Println("new node is a right child")
				newNode.parent.color = Black
				newNode.parent.parent.color = Red
				tree.leftRotate(newNode.parent.parent)
			}
		}
	}
	tree.root.color = Black
	fmt.Println("Violation fixed")
}

func (tree *RBTree) leftRotate(rotatingNode *node) {
	rightTree := rotatingNode.right
	rotatingNode.right = rightTree.left

	if rightTree.left != nil {
		rightTree.left.parent = rotatingNode
	}

	rightTree.parent = rotatingNode.parent

	if rotatingNode.parent == nil {
		tree.root = rightTree
	} else if rotatingNode == rotatingNode.parent.left {
		rotatingNode.parent.left = rightTree
	} else {
		rotatingNode.parent.right = rightTree
	}

	rightTree.left = rotatingNode
	rotatingNode.parent = rightTree
}

func (tree *RBTree) rightRotate(rotatingNode *node) {
	leftTree := rotatingNode.left
	rotatingNode.left = leftTree.right

	if rotatingNode.left != nil {
		rotatingNode.left.parent = rotatingNode
	}

	leftTree.parent = rotatingNode.parent

	if rotatingNode.parent == nil {
		tree.root = leftTree
	} else if rotatingNode == rotatingNode.parent.left {
		rotatingNode.parent.left = leftTree
	} else {
		rotatingNode.parent.right = leftTree
	}

	leftTree.right = rotatingNode
	rotatingNode.parent = leftTree
}

func (tree *RBTree) transplant(u *node, v *node) {
	if u.parent == nil {
		tree.root = v
	} else if u == u.parent.left {
		u.parent.left = v
	} else {
		u.parent.right = v
	}
	if v != nil {
		v.parent = u.parent
	}
}

func (tree *RBTree) getMinimum(currNode *node) *node {
	for currNode.left != nil {
		currNode = currNode.left
	}
	return currNode
}

func (tree *RBTree) Expire(key string, timestamp uint64, activeTxns map[uint64]bool) error {
	tree.Lock()
	defer tree.Unlock()
	delNode := tree.Search(tree.root, key)
	if delNode != nil {
		delNode.data.expired = timestamp
		return nil
	}
	return errors.New("could not expire node that didnt exist")
}

func (tree *RBTree) Delete(key string) {
	tree.Lock()
	defer tree.Unlock()
	delNode := tree.Search(tree.root, key)
	tree.deleteNode(delNode)
}

func (tree *RBTree) deleteNode(delNode *node) {
	copyNode := delNode
	copyOriginalColor := delNode.color
	var x *node

	if delNode.left == nil {
		x = delNode.right
		tree.transplant(delNode, delNode.right)
	} else if delNode.right == nil {
		x = delNode.left
		tree.transplant(delNode, delNode.left)
	} else {
		copyNode = tree.getMinimum(delNode.right)
		copyOriginalColor = copyNode.color
		x = copyNode.right
		if copyNode.parent == delNode {
			x.parent = copyNode
		} else {
			tree.transplant(copyNode, copyNode.right)
			copyNode.right = delNode.right
			copyNode.right.parent = copyNode
		}
		tree.transplant(delNode, copyNode)
		copyNode.left = delNode.left
		copyNode.left.parent = copyNode
		copyNode.color = delNode.color
	}
	if copyOriginalColor == Black {
		tree.deleteFixUp(x)
	}
}

func (tree *RBTree) deleteFixUp(fixNode *node) {
	for fixNode != tree.root && fixNode.color == Black {
		if fixNode == fixNode.parent.left {
			w := fixNode.parent.right
			if w.color == Red {
				w.color = Black
				fixNode.parent.color = Red
				tree.leftRotate(fixNode.parent)
				w = fixNode.parent.right
			}
			if w.left.color == Black && w.right.color == Black {
				w.color = Red
				fixNode = fixNode.parent
			} else {
				if w.right.color == Black {
					w.left.color = Black
					w.color = Red
					tree.rightRotate(w)
					w = fixNode.parent.right
				}
				w.color = fixNode.parent.color
				fixNode.parent.color = Black
				w.right.color = Black
				tree.leftRotate(fixNode.parent)
				fixNode = tree.root
			}
		} else {
			w := fixNode.parent.left
			if w.color == Red {
				w.color = Black
				fixNode.parent.color = Red
				tree.rightRotate(fixNode.parent)
				w = fixNode.parent.left
			}
			if w.right.color == Black && w.left.color == Black {
				w.color = Red
				fixNode = fixNode.parent
			} else {
				if w.right.color == Black {
					w.right.color = Black
					w.color = Red
					tree.leftRotate(w)
					w = fixNode.parent.left
				}
				w.color = fixNode.parent.color
				fixNode.parent.color = Black
				w.left.color = Black
				tree.rightRotate(fixNode.parent)
				fixNode = tree.root
			}
		}
	}
	fixNode.color = Black
}

func (tree *RBTree) BreadthFirstTraversal() {
	tree.RLock()
	defer tree.RUnlock()
	if tree.root == nil {
		return
	}
	nodes := make([]node, 1)
	nodes[0] = *tree.root
	for len(nodes) != 0 {
		currentNode := nodes[0]
		if currentNode.data.key == (*tree).root.data.key {
			fmt.Printf("%s (%d)\n", currentNode.data.key, currentNode.color)
		} else {
			fmt.Printf("%s (%d) -> %s (%d)\n", currentNode.parent.data.key, currentNode.parent.color, currentNode.data.key, currentNode.color)
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

func (tree *RBTree) InOrderTraversal() {
	tree.RLock()
	defer tree.RUnlock()
	tree.inOrderTraversal(tree.root)
}

func (tree *RBTree) inOrderTraversal(currNode *node) {
	if currNode == nil {
		return
	}

	tree.inOrderTraversal(currNode.left)

	if currNode.color == Black {
		fmt.Printf("%s: Black\n", currNode.data.key)
	} else {
		fmt.Printf("%s: Red\n", currNode.data.key)
	}

	tree.inOrderTraversal(currNode.right)
}

// Returns true if tree is sorted properly
func (tree *RBTree) Sorted() bool {
	tree.RLock()
	defer tree.RUnlock()
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
