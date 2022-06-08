package main

import (
	"errors"
	"math"
)

type Value = [10]byte

type KDTree struct {
	kSize   int
	maxSize uint64
	size    uint64 // current size in bytes
	root    *Node
}

func (t *KDTree) Put(key *Point, value Value) error {

	if key.GetSize() != t.kSize {
		return errors.New("Key and Tree have different sizes!")
	}

	err, node := NewNode(key, value)

	if err != nil {
		return err
	}

	if t.root == nil {
		t.root = node
		t.size += node.GetByteSize()
		return nil
	}

	currentNode := t.root

	for depth := 0; ; depth++ {

		keyIndex := depth % t.kSize

		if currentNode.KeyValueAt(keyIndex) < node.KeyValueAt(keyIndex) {
			if currentNode.Right == nil {
				currentNode.Right = node
				t.size += node.GetByteSize()
				return nil
			}

			currentNode = currentNode.Right

		} else {
			if currentNode.Left == nil {
				currentNode.Left = node
				t.size += node.GetByteSize()
				return nil
			}

			currentNode = currentNode.Left

		}
	}
}

func (t *KDTree) Get(key *Point) ([]Value, error) {

	if key.IsPartial() {
		return t.partialSearchQuery(0, key, t.root), nil
	}

	_, _, node := t.searchQuery(key)
	if node == nil {
		return make([]Value, 0, 0), errors.New("Couldn't find key")
	}

	return []Value{node.GetValue()}, nil
}

func (t *KDTree) Delete(key *Point) error {

	depth, parent, node := t.searchQuery(key)
	return t.deleteNode(parent, node, depth)
}

func (t *KDTree) deleteNode(parent *Node, node *Node, depth int) error {

	if node == nil {
		return errors.New("node to delete not found")
	}

	if node.IsLeaf() {
		if parent.IsLeftChild(node) {
			parent.Left = nil
		} else {
			parent.Right = nil
		}

	} else if node.Right != nil {

		keyIndex := depth % t.kSize
		depth, _, minNode := t.searchMinimum(node.Right, keyIndex, 0)
		_, minParent, _ := t.searchQuery(&minNode.Key)

		if minNode != nil {

			t.deleteNode(minParent, minNode, depth)

			// put min at top

			minNode.Right = node.Right
			minNode.Left = node.Left
			if t.root == node {
				t.root = minNode
			} else if parent.IsLeftChild(node) {
				parent.Left = minNode
			} else {
				parent.Right = minNode
			}
		}

	} else if node.Left != nil {

		keyIndex := depth % t.kSize
		//TODO change to search maximum
		depth, _, maxNode := t.searchMaximum(node.Left, keyIndex, 0)
		_, maxParent, _ := t.searchQuery(&maxNode.Key)

		if maxNode != nil {

			t.deleteNode(maxParent, maxNode, depth)

			// put min at top
			maxNode.Right = node.Right
			maxNode.Left = node.Left
			if t.root == node {
				t.root = maxNode
			} else if parent.IsLeftChild(node) {
				parent.Left = maxNode
			} else {
				parent.Right = maxNode
			}
		}
	}

	t.size += node.GetByteSize()

	return nil
}

func (t *KDTree) searchMinimum(n *Node, keyIndex int, depthParam int) (int, *Node, *Node) {
	var (
		parentLeft  *Node = nil
		parentRight *Node = nil
		nodeLeft    *Node = nil
		nodeRight   *Node = nil
		depthLeft   int   = 0
		depthRight  int   = 0
	)

	if n.Left != nil {
		depthLeft, parentLeft, nodeLeft = t.searchMinimum(n.Left, keyIndex, depthParam+1)
	}

	if n.Right != nil {
		depthRight, parentRight, nodeRight = t.searchMinimum(n.Right, keyIndex, depthParam+1)
	}

	if nodeRight == nil && nodeLeft == nil {
		return depthParam, nil, n
	}

	if nodeRight == nil {
		if nodeLeft.SmallerThan(n, keyIndex) {
			return depthLeft, parentLeft, nodeLeft
		} else {
			return depthParam, nil, n
		}
	}

	if nodeLeft == nil {
		if nodeRight.SmallerThan(n, keyIndex) {
			return depthRight, parentRight, nodeRight
		} else {
			return depthParam, nil, n
		}
	}

	minNode := getMin(nodeLeft, nodeRight, n, keyIndex)
	switch minNode {
	case n:
		return depthParam, nil, n
	case nodeLeft:
		return depthLeft, nil, nodeLeft
	case nodeRight:
		return depthRight, nil, nodeRight
	}
	return 0, nil, nil
}

func getMin(node1 *Node, node2 *Node, node3 *Node, keyIndex int) *Node {
	if node1.SmallerThan(node2, keyIndex) && node1.SmallerThan(node3, keyIndex) {
		return node1
	}
	if node2.SmallerThan(node1, keyIndex) && node2.SmallerThan(node3, keyIndex) {
		return node2
	}
	if node3.SmallerThan(node1, keyIndex) && node3.SmallerThan(node2, keyIndex) {
		return node3
	}
	return nil
}

func (t *KDTree) searchMaximum(n *Node, keyIndex int, depthParam int) (int, *Node, *Node) {
	//TODO also keep track of the parent
	var (
		parentLeft  *Node = nil
		parentRight *Node = nil
		nodeLeft    *Node = nil
		nodeRight   *Node = nil
		depthLeft   int   = 0
		depthRight  int   = 0
	)

	if n.Left != nil {
		depthLeft, parentLeft, nodeLeft = t.searchMaximum(n.Left, keyIndex, depthParam+1)
	}

	if n.Right != nil {
		depthRight, parentRight, nodeRight = t.searchMaximum(n.Right, keyIndex, depthParam+1)
	}

	if nodeRight == nil && nodeLeft == nil {
		return depthParam, nil, n
	}

	if nodeRight == nil {
		return depthLeft, parentLeft, nodeLeft
	}

	if nodeLeft == nil {
		return depthRight, parentRight, nodeRight
	}

	if nodeLeft.KeyValueAt(keyIndex) > nodeRight.KeyValueAt(keyIndex) {
		return depthLeft, parentLeft, nodeLeft
	}

	return depthRight, parentRight, nodeRight
}

func (t *KDTree) Scan(from *Point, to *Point) ([]Value, error) {

	if (from != nil && from.GetSize() != t.kSize) || (to != nil && to.GetSize() != t.kSize) {
		return make([]Value, 0), errors.New("wrong key size")
	}

	result := t.scanQuery(t.root, from, to, 0)

	return result, nil
}

func (t *KDTree) scanQuery(node *Node, from *Point, to *Point, depth int) []Value {

	values := make([]Value, 0, 10)

	if node == nil {
		return values
	}

	keyIndex := depth % t.kSize
	nodeKey := node.KeyValueAt(keyIndex)

	var fromK uint64 = 0
	var toK uint64 = math.MaxUint64

	if from != nil {
		_, tmpK := from.GetKeyAt(keyIndex)
		if tmpK.IsSome {
			fromK = tmpK.Value
		}
	}

	if to != nil {
		_, tmpK := to.GetKeyAt(keyIndex)
		if tmpK.IsSome {
			toK = tmpK.Value
		}
	}

	branchesToVisit := 0

	if nodeKey >= fromK {
		result := t.scanQuery(node.Left, from, to, depth+1)
		values = append(values, result...)
		branchesToVisit++
	}

	if nodeKey <= toK {
		result := t.scanQuery(node.Right, from, to, depth+1)
		values = append(values, result...)
		branchesToVisit++
	}


	if branchesToVisit == 2 && node.Key.IsWithin(from, to) {
		values = append(values, node.GetValue())
	}

	return values
}

func (t *KDTree) GetNN(key *Point) (Value, error) {

	if t.root == nil {
		return *new(Value), errors.New("Tree is empty!")
	}

	if key == nil || key.GetSize() != int(t.kSize) {
		return *new(Value), errors.New("Wrong or nil key!")
	}

	nearestNode := t.nearestNeighbour(t.root, key, 0)

	return nearestNode.GetValue(), nil
}

func (t *KDTree) nearestNeighbour(node *Node, key *Point, depth int) *Node {

	if node == nil {
		return nil
	}

	keyIndex := depth % t.kSize

	nodeKeyValue := node.KeyValueAt(keyIndex)
	_, kv := key.GetKeyAt(keyIndex)

	var nextBranch *Node
	var alternativeBranch *Node

	if kv.Value > nodeKeyValue {
		nextBranch = node.Right
		alternativeBranch = node.Left
	} else {
		nextBranch = node.Left
		alternativeBranch = node.Right
	}

	tmp := t.nearestNeighbour(nextBranch, key, depth+1)
	closest := findClosest(key, tmp, node)

	if closest == nil {
		return nil
	}

	_, distanceToBest := key.GetDistance(&closest.Key)

	dist := math.Abs(float64(nodeKeyValue) - float64(kv.Value))

	if distanceToBest >= dist {
		tmp = t.nearestNeighbour(alternativeBranch, key, depth+1)
		closest = findClosest(key, tmp, closest)
	}

	return closest
}

func findClosest(key *Point, node1 *Node, node2 *Node) *Node {

	dist1 := math.MaxFloat64
	dist2 := math.MaxFloat64

	if node1 != nil {
		_, dist1 = key.GetDistance(&node1.Key)
	}
	if node2 != nil {
		_, dist2 = key.GetDistance(&node2.Key)
	}

	if dist1 < dist2 {
		return node1
	}

	return node2
}

func (t *KDTree) Upsert(key *Point, value Value) error {

	if key.GetSize() != t.kSize || key.IsPartial() {
		return errors.New("Wrong key!")
	}

	_, _, node := t.searchQuery(key)

	if node == nil {
		return errors.New("Couldnt find node to upsert")
	}

	node.SetValue(value)

	return nil
}

func NewKDTree(keySize int, maxSize uint64) (*KDTree, error) {
	if keySize < 1 {
		return nil, errors.New("key size has to be at least 1")
	}

	return &KDTree{kSize: keySize, maxSize: maxSize, size: 4 * 8, root: nil}, nil
}

// returns found Node and parent of found Node
func (t *KDTree) searchQuery(key *Point) (int, *Node, *Node) {

	var parentNode *Node = nil
	currentNode := t.root

	for depth := 0; ; depth++ {

		if currentNode.Key.IsEqual(key) {
			return depth, parentNode, currentNode
		}

		keyIndex := depth % t.kSize

		_, kv := key.GetKeyAt(keyIndex)

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

func (t *KDTree) partialSearchQuery(depth int, key *Point, node *Node) []Value {

	// reserve size 10
	values := make([]Value, 0, 10)

	if node == nil {
		return values
	}

	if node.Key.IsPartiallyEqual(key) {
		values = append(values, node.GetValue())
	}

	keyIndex := depth % t.kSize

	_, kv := key.GetKeyAt(keyIndex)

	nodeKeyValue := node.KeyValueAt(keyIndex)

	if !kv.IsSome || nodeKeyValue >= kv.Value {
		resultLeft := t.partialSearchQuery(depth+1, key, node.Left)
		values = append(values, resultLeft...)
	}

	if !kv.IsSome || nodeKeyValue < kv.Value {
		resultRight := t.partialSearchQuery(depth+1, key, node.Right)
		values = append(values, resultRight...)
	}

	return values
}

func (t *KDTree) GetNodesCount() int {

	return countSubTreesNodes(t.root) + 1
}

func countSubTreesNodes(n *Node) int {

	count := 0

	if n.Left != nil {
		count++
		count += countSubTreesNodes(n.Left)
	}

	if n.Right != nil {
		count++
		count += countSubTreesNodes(n.Right)
	}

	return count
}
