package rbTree

import "testing"

func TestDoubleInsert(t *testing.T) {
	tree := NewTree()
	tree.Set("key1", "bananas", 1)
	tree.Set("key2", "apples", 1)
}

func TestLevelOrderTraversal(t *testing.T) {
	tree := NewTree()
	tree.Set("goolash", "2", 1)
	tree.BreadthFirstTraversal()
	tree.Set("piper", "1", 1)
	tree.BreadthFirstTraversal()
	tree.Set("banana", "2", 1)
	tree.BreadthFirstTraversal()
	tree.Set("apple", "1", 1)
	tree.BreadthFirstTraversal()
	tree.Set("squash", "1", 1)
	tree.BreadthFirstTraversal()
	tree.Set("pizza", "2", 1)
	tree.BreadthFirstTraversal()
	tree.Set("yellow", "2", 1)
	tree.BreadthFirstTraversal()
}

func TestSetToUpdate(t *testing.T) {
	tree := NewTree()
	tree.Set("goolash", "2", 1)
	tree.Set("piper", "1", 1)

	tree.Set("goolash", "3", 1)
	keyVal, err := tree.Get("goolash", 1)
	if keyVal != "3" {
		t.Error(err)
	}

	tree.Set("piper", "4", 1)
	keyVal, err = tree.Get("piper", 1)
	if keyVal != "4" {
		t.Error(err)
	}
}

func TestDeletion(t *testing.T) {
	tree := NewTree()
	tree.Set("goolash", "2", 1)
	tree.BreadthFirstTraversal()
	tree.Set("piper", "1", 1)
	tree.BreadthFirstTraversal()
	tree.Set("banana", "2", 1)
	tree.BreadthFirstTraversal()
	tree.Set("apple", "1", 1)
	tree.BreadthFirstTraversal()
	tree.Set("squash", "1", 1)
	tree.BreadthFirstTraversal()
	tree.Delete("goolash")
	tree.BreadthFirstTraversal()
	tree.Set("yellow", "2", 1)
	tree.BreadthFirstTraversal()
}
