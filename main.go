package main

import "fmt"

type record struct {
	key   string
	value string
}

type nodeColor int

const (
	Red nodeColor = iota
	Black
)

type node struct {
	color  nodeColor
	data   record
	left   *node
	right  *node
	parent *node
}

type RBTree struct {
	root *node
}

func (tree *RBTree) Insert(newKV record) {
	newNode := node{}
	newNode.data = newKV
	newNode.color = Red

	tree.insertHelper(tree.root, &newNode)

	tree.fixViolation(&newNode)
}

func (tree *RBTree) insertHelper(root *node, newNode *node) *node {
	if root == nil {
		fmt.Println("appending new node")
		return newNode
	}

	if newNode.data.key < root.data.key {
		fmt.Println("new node is less than root key")
		root.left = tree.insertHelper(root.left, newNode)
		root.left.parent = root
	} else if newNode.data.key > root.data.key {
		fmt.Println("new node key greater than root key")
		root.right = tree.insertHelper(root.right, newNode)
		root.right.parent = root
	}

	return root
}

func (tree *RBTree) fixViolation(newNode *node) {
	fmt.Println("Starting to fix the violation")
	for newNode.parent != nil && newNode.parent.color == Red { //newNode.parent.color called on root node returning error
		fmt.Println("In loop")
		if newNode.parent == newNode.parent.parent.left {
			fmt.Println("parent is left child")
			uncle := newNode.parent.parent.right
			if uncle.color == Red {
				fmt.Println("uncle is red")
				newNode.parent.color = Black
				uncle.color = Black
				newNode.parent.parent.color = Red
				newNode = newNode.parent.parent
			} else {
				fmt.Println("uncle is black")
				if newNode == newNode.parent.right {
					fmt.Println("new node is a left child")
					newNode = newNode.parent
					tree.leftRotate(newNode)
				}
				fmt.Println("new node is a right child")
				newNode.parent.color = Black
				newNode.parent.parent.color = Red
				tree.rightRotate(newNode.parent.parent)
			}
		} else {
			fmt.Println("parent is right child")
			uncle := newNode.parent.parent.left
			if uncle.color == Red {
				fmt.Println("uncle is red")
				newNode.parent.color = Black
				uncle.color = Black
				newNode.parent.parent.color = Red
				newNode = newNode.parent.parent
			} else {
				fmt.Println("uncle is black")
				if newNode == newNode.parent.left {
					fmt.Println("new node is a left child")
					newNode = newNode.parent
					tree.leftRotate(newNode)
				}
				fmt.Println("new node is a right child")
				newNode.parent.color = Black
				newNode.parent.parent.color = Red
				tree.rightRotate(newNode.parent.parent)
			}
		}
	}
	tree.root.color = Black
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

func main() {
	tree := RBTree{}

	rootNode := node{}
	rootNode.color = Black

	rootData := record{}
	rootData.key = "5"
	rootData.value = "value2"

	rootNode.data = rootData
	tree.root = &rootNode

	newRecord := record{}
	newRecord.key = "2"
	newRecord.value = "value1"

	newRecordz := record{}
	newRecordz.key = "9"
	newRecordz.value = "value1"

	newRecordf := record{}
	newRecordf.key = "8"
	newRecordf.value = "value1"

	fmt.Printf("Inserting key 2\n")
	tree.Insert(newRecord)
	tree.inOrderTraversal(tree.root)

	fmt.Printf("Inserting key 9\n")
	tree.Insert(newRecordz)
	tree.inOrderTraversal(tree.root)

	fmt.Printf("Inserting key 8\n")
	tree.Insert(newRecordf)
	tree.inOrderTraversal(tree.root)

	// fmt.Printf("root node is: %s", tree.root.data.key)

	// tree.leftRotate(tree.root)

	// fmt.Printf("root after left rotation: %s", tree.root.data.key)

	// tree.rightRotate(tree.root)

	// fmt.Printf("Root after right rotation: %s", tree.root.data.key)

}
