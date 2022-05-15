package main

import (
	"errors"
)

type KDTree[T StorableType] struct {
	kSize int
	root *Node[T]
}

func (t * KDTree[T]) Put(key *Point, value T) error {

	if key.GetSize() != t.kSize {
		return errors.New("Key and Tree have different sizes!")
	}

	err, node := NewNode(key, value)

	if err != nil {
		return err
	}

	if t.root == nil {
		t.root = node
		return nil
	}

	currentNode := t.root

	for depth := 0;; depth++  {

		keyIndex := depth % t.kSize

		if currentNode.KeyValueAt(keyIndex) < node.KeyValueAt(keyIndex) {
			if currentNode.Right == nil {
				currentNode.Right = node
				return nil	
			} 

			currentNode = currentNode.Right	

		} else {
			if currentNode.Left == nil {
				currentNode.Left = node
				return nil	
			} 

			currentNode = currentNode.Left

		}
	}
}

func (t * KDTree[T]) Get(key *Point) ([]T, error)  {

	if key.IsPartial() {
		return t.partialSearchQuey(key)
	}

	_,_, node := t.searchQuery(key)
	if node == nil {
		return make([]T, 0, 0), errors.New("Couldn't find key")
	}

	return []T{node.GetValue()}, nil
}

func (t * KDTree[T]) Delete(key * Point) error  {

	depth, parent, node := t.searchQuery(key)

	if node == nil {
		return errors.New("node to delete not found!")
	}

	t.deleteNode(parent, node, depth)

	return nil
}

func (t * KDTree[T]) deleteNode(parent *Node[T], node *Node[T], depth int) {

	if node == nil {
		return
	}

	if node.IsLeaf() {
		if parent.IsLeftChild(node) {
			parent.Left = nil
		} else {
			parent.Right = nil
		} 

	} else if node.Right != nil {

		keyIndex := depth % t.kSize
		depth, minParent, minNode := t.searchMinimum(node.Right, keyIndex, 0)

		if minNode != nil {

		t.deleteNode(minParent, minNode, depth)

		// put min at top
		minNode.Right = node.Right
		minNode.Left = node.Left
	}

	} else if node.Left != nil {

		keyIndex := depth % t.kSize
		depth, minParent, minNode := t.searchMinimum(node.Left, keyIndex, 0)

		if minNode != nil {

		t.deleteNode(minParent, minNode, depth)

		// put min at top
		minNode.Right = node.Right
		minNode.Left = node.Left

		// swap
		refTmp := minParent.Left
		minParent.Left = minParent.Right
		minParent.Right = refTmp
	}
	}
}

func (t * KDTree[T]) searchMinimum(n * Node[T], keyIndex int, depthParam int) (int, *Node[T], *Node[T]) {

	var (
		parentLeft *Node[T] = nil
		parentRight *Node[T] = nil
		nodeLeft *Node[T] =  nil
		nodeRight *Node[T] = nil
		depthLeft int = 0
		depthRight int = 0

	)

	if n.Left != nil {
		depthLeft, parentLeft, nodeLeft = t.searchMinimum(n.Left, keyIndex, depthParam + 1)
	}

	if n.Right != nil {
		depthRight, parentRight, nodeRight = t.searchMinimum(n.Right, keyIndex, depthParam + 1)
	}


	if nodeRight == nil {
		return depthRight, parentLeft, nodeLeft
	}

	if nodeLeft == nil {
		return depthRight, parentRight, nodeRight
	}

	if  nodeLeft.KeyValueAt(keyIndex) < nodeRight.KeyValueAt(keyIndex) {
		return depthLeft, parentLeft, nodeLeft
	}

	return  depthRight, parentRight, nodeRight
}


func (t * KDTree[T]) Scan(options *Range) ([]T, error)  {
	return make([]T, 0), nil
}

func (t * KDTree[T]) GetNN(key *Point) (T, error) {
	return "", nil
}

func (t * KDTree[T]) Upsert(key *Point, value T) error {
	return nil
}

func NewKDTree[T StorableType](size int) (*KDTree[T], error) {	
	if size < 1 {
		return nil, errors.New("key size has to be at least 1")
	}
	
	return &KDTree[T]{kSize: size, root: nil}, nil
}

// returns found Node and parent of found Node
func (t *KDTree[T]) searchQuery(key *Point) (int, *Node[T], *Node[T]){

	var parentNode *Node[T] = nil
	currentNode := t.root


	for depth := 0;; depth++ {

		if currentNode.Key.IsEqual(key) {
			return depth, parentNode, currentNode
		}

		keyIndex := depth % t.kSize

		_,kv := key.GetKeyAt(keyIndex)

		if currentNode.KeyValueAt(keyIndex) < kv.Value {
			if currentNode.Right == nil {
				return depth, currentNode, nil
			}

			parentNode = currentNode
			currentNode = currentNode.Right

		} else {
			if currentNode.Left == nil {
				return depth, currentNode, nil
			}

			parentNode = currentNode
			currentNode = currentNode.Left

		}
	}
}

func (t *KDTree[T]) partialSearchQuey(k *Point) ([]T, error) {
	return make([]T, 0, 0), nil
}


func (t*KDTree[T]) GetNodesCount() int {

	return countSubTreesNodes(t.root) + 1
}


func countSubTreesNodes[T StorableType](n *Node[T]) int {

	count := 0

	if n.Left != nil {		
		count ++
		count += countSubTreesNodes(n.Left)
	}

	if n.Right != nil {
		count ++
		count += countSubTreesNodes(n.Right)
	}

	return count
}


