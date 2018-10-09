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

func (tree *RBTree) Search(root *node, key string) *node {
	if root == nil || key == root.data.key {
		return root
	}
	if key < tree.root.data.key {
		return tree.Search(root.left, key)
	} else {
		return tree.Search(root.right, key)
	}
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

func (tree *RBTree) Delete(delNode *node) {
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

	newRecorde := record{}
	newRecorde.key = "3"
	newRecorde.value = "value3"

	fmt.Printf("Inserting key 2\n")
	tree.Insert(newRecord)
	tree.inOrderTraversal(tree.root)

	fmt.Printf("Inserting key 9\n")
	tree.Insert(newRecordz)
	tree.inOrderTraversal(tree.root)

	fmt.Printf("Inserting key 8\n")
	tree.Insert(newRecordf)
	tree.inOrderTraversal(tree.root)

	fmt.Printf("Inserting key 3\n")
	tree.Insert(newRecorde)
	tree.inOrderTraversal(tree.root)

	fmt.Printf("Deleting key 9\n")
	nodeToDel := tree.Search(tree.root, "9")
	tree.Delete(nodeToDel)
	tree.inOrderTraversal(tree.root)

	fmt.Printf("Deleting key 8\n")
	nodeToDel = tree.Search(tree.root, "8")
	tree.Delete(nodeToDel)
	tree.inOrderTraversal(tree.root)

	fmt.Printf("Deleting key 5\n")
	nodeToDel = tree.Search(tree.root, "5")
	tree.Delete(nodeToDel)
	tree.inOrderTraversal(tree.root)

	// fmt.Printf("root node is: %s", tree.root.data.key)

	// tree.leftRotate(tree.root)

	// fmt.Printf("root after left rotation: %s", tree.root.data.key)

	// tree.rightRotate(tree.root)

	// fmt.Printf("Root after right rotation: %s", tree.root.data.key)

}
