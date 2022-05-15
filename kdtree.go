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

	return nil
}

func (t * KDTree[T]) Get(key *Point) ([]T, error)  {

	if key.IsPartial() {
		return t.partialSearchQuey(key)
	}

	node := t.searchQuery(key)
	if node == nil {
		return make([]T, 0, 0), errors.New("Couldn't find key")
	}

	return []T{node.GetValue()}, nil


	return make([]T, 0, 0), nil
}

func (t * KDTree[T]) Delete(key * Point) error  {
	return nil
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


func (t *KDTree[T]) searchQuery(key *Point) *Node[T]{

	currentNode := t.root


	for depth := 0;; depth++ {

		if currentNode.Key.IsEqual(key) {
			return currentNode
		}

		keyIndex := depth % t.kSize

		_,kv := key.GetKeyAt(keyIndex)

		if currentNode.KeyValueAt(keyIndex) < kv.Value {
			if currentNode.Right == nil {
				return nil
			}

			currentNode = currentNode.Right

		} else {
			if currentNode.Left == nil {
				return nil
			}

			currentNode = currentNode.Left

		}


	}


	return nil
}

func (t *KDTree[T]) partialSearchQuey(k *Point) ([]T, error) {
	return make([]T, 0, 0), nil
}




