package main

import (
	"errors"
)

type Node struct {
	Key Point
	value Value

	Left *Node
	Right *Node
}



func NewNode(key *Point, value Value) (error, *Node) {

	if key.IsPartial() {
		return errors.New("cannot store partial point"), nil
	}


	return nil, &Node{Key: *key, value: value, Left: nil, Right: nil}
}

func (n *Node) IsLeftChild(nc *Node) bool {
	return n.Key.IsEqual(&n.Left.Key)
}

func (n *Node) IsLeaf() bool {
	return n.Left == nil && n.Right == nil
}

func (n *Node) GetValue() Value {
	return n.value
}

func (n *Node) KeyValueAt(i int) uint64 {
	// already know
	// partial keys cannot
	// be stored so we ignore error
	_, v := n.Key.GetKeyAt(i)
	return v.Value
}

func (n *Node) GetByteSize() uint64 {
	return n.Key.GetByteSize() + 10 + 8 + 8
}


