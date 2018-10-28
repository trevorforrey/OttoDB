package rbTree

import "testing"

func TestDoubleInsert(t *testing.T) {
	tree := NewTree()
	tree.Set("key1", "bananas")
	tree.Set("key2", "apples")
}

func TestLevelOrderTraversal(t *testing.T) {
	tree := NewTree()
	tree.Set("goolash", "2")
	tree.BreadthFirstTraversal()
	tree.Set("piper", "1")
	tree.BreadthFirstTraversal()
	tree.Set("banana", "2")
	tree.BreadthFirstTraversal()
	tree.Set("apple", "1")
	tree.BreadthFirstTraversal()
	tree.Set("squash", "1")
	tree.BreadthFirstTraversal()
	tree.Set("pizza", "2")
	tree.BreadthFirstTraversal()
	tree.Set("yellow", "2")
	tree.BreadthFirstTraversal()
}

func TestSetToUpdate(t *testing.T) {
	tree := NewTree()
	tree.Set("goolash", "2")
	tree.Set("piper", "1")

	tree.Set("goolash", "3")
	if tree.Get("goolash") != "3" {
		t.Error("goolash value not updated")
	}

	tree.Set("piper", "4")
	if tree.Get("piper") != "4" {
		t.Error("piper value not updated")
	}
}

func TestDeletion(t *testing.T) {
	tree := NewTree()
	tree.Set("goolash", "2")
	tree.BreadthFirstTraversal()
	tree.Set("piper", "1")
	tree.BreadthFirstTraversal()
	tree.Set("banana", "2")
	tree.BreadthFirstTraversal()
	tree.Set("apple", "1")
	tree.BreadthFirstTraversal()
	tree.Set("squash", "1")
	tree.BreadthFirstTraversal()
	tree.Delete("goolash")
	tree.BreadthFirstTraversal()
	tree.Set("yellow", "2")
	tree.BreadthFirstTraversal()
}
