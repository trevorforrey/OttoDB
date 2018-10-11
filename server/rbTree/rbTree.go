package rbTree

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

func NewTree() *RBTree {
	tree := RBTree{}
	return &tree
}

func (tree *RBTree) Get(key string) string {
	fmt.Println("About to start tree search")
	getNode := tree.Search(tree.root, key)
	fmt.Printf("Found key: %s\n", getNode.data.key)
	fmt.Printf("Going to send value: %s\n", getNode.data.value)
	return getNode.data.value
}

func (tree *RBTree) Set(key string, value string) {
	var newRecord record
	newRecord.key = key
	newRecord.value = value
	tree.Insert(newRecord)
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

func (tree *RBTree) Insert(newKV record) {
	newNode := node{}
	newNode.data = newKV
	newNode.color = Red

	if tree.root == nil {
		newNode.color = Black
		tree.root = &newNode
	} else {
		tree.insertHelper(tree.root, &newNode)
		tree.fixViolation(&newNode)
	}
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
	for newNode.parent != nil && newNode.parent.color == Red {
		fmt.Println("In loop")
		if newNode.parent == newNode.parent.parent.left {
			fmt.Println("parent is left child")
			uncle := newNode.parent.parent.right
			if uncle != nil && uncle.color == Red {
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
			if uncle != nil && uncle.color == Red {
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
					tree.rightRotate(newNode)
				}
				fmt.Println("new node is a right child")
				newNode.parent.color = Black
				newNode.parent.parent.color = Red
				tree.leftRotate(newNode.parent.parent)
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

func (tree *RBTree) Delete(key string) {
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
	if tree.root == nil {
		return
	}
	nodes := make([]node, 1)
	nodes[0] = *tree.root
	for len(nodes) != 0 {
		currentNode := nodes[0]
		if currentNode == *tree.root {
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
