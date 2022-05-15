package main

import (
	"errors"
)


type Node[T StorableType] struct {
	Key Point
	value T

	Left *Node[T]
	Right *Node[T]
}



func NewNode[T StorableType](key *Point, value T) (error, *Node[T]) {

	if key.IsPartial() {
		return errors.New("cannot store partial point"), nil
	}


	return nil, &Node[T]{Key: *key, value: value, Left: nil, Right: nil}

}

func (n *Node[T]) GetValue() T {
	return n.value
}

func (n *Node[T]) KeyValueAt(i int) uint64 {
	// already know
	// partial keys cannot
	// be stored so we ignore error
	_, v := n.Key.GetKeyAt(i)
	return v.Value
}
