/**
kv_store.go
Author: Tobias Famos & Mattia Pedrazzi
*/

package main

type StorableType interface {
	string 
}


type KVStoreOptions struct {
	kSize int // key size
	size  int // Store size
}

type Range struct {
	minKey Point
	maxKey Point
}

type KVStore [T StorableType] interface {
	Put(key *Point, value T) error
	Get(key *Point) ([]T, error) // exact match query and partial matches
	Delete(key * Point) error 
	Scan(options *Range) ([]T, error) // range query
	GetNN(key *Point) (T, error) // nearest neighbour query
	Upsert(key *Point, value T) error
}

type KVStoreMock[T StorableType] struct {
	kSize int
	size     int
	// TODO
        // tree KDTree	
}

func NewKVStore[T StorableType](options *KVStoreOptions) (KVStore[T], error) {
	if options.size == 0 {
		options.size = 2048
	}

	return &KVStoreMock[T]{
		kSize: options.kSize,
		size: options.size,
	}, nil
}

func (k *KVStoreMock[T]) Open() error {
	return nil
}

func (k *KVStoreMock[T]) Close() error {
	return nil
}

func (k *KVStoreMock[T]) Delete(key *Point) error {
	return nil
}

func (k *KVStoreMock[T]) Get(key *Point) ([]T, error) {
	return make([]T, 0), nil
}

func (k *KVStoreMock[T]) GetNN(key *Point) (T, error) {
	return "", nil
}

func (k *KVStoreMock[T]) Put(key *Point, value T) error {
	return nil
}

func (k *KVStoreMock[T]) Upsert(key *Point, value T) error {
	return nil
}

func (k *KVStoreMock[T]) Scan(*Range) ([]T, error) {
	return make([]T, 0), nil
}
