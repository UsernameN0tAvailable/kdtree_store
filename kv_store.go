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

type KVStore interface {
	Put(key *Point, value Value) error
	Get(key *Point) ([]Value, error) // exact match query and partial matches
	Delete(key * Point) error 
	Scan(options *Range) ([]Value, error) // range query
	GetNN(key *Point) (Value, error) // nearest neighbour query
	Upsert(key *Point, value Value) error
}

type KVStoreMock struct {
	kSize int
	size     int
}

func NewKVStore(options *KVStoreOptions) (KVStore, error) {
	if options.size == 0 {
		options.size = 2048
	}

	return &KVStoreMock{
		kSize: options.kSize,
		size: options.size,
	}, nil
}

func (k *KVStoreMock) Open() error {
	return nil
}

func (k *KVStoreMock) Close() error {
	return nil
}

func (k *KVStoreMock) Delete(key *Point) error {
	return nil
}

func (k *KVStoreMock) Get(key *Point) ([]Value, error) {
	return make([]Value, 0), nil
}

func (k *KVStoreMock) GetNN(key *Point) (Value, error) {
	return *new(Value), nil
}

func (k *KVStoreMock) Put(key *Point, value Value) error {
	return nil
}

func (k *KVStoreMock) Upsert(key *Point, value Value) error {
	return nil
}

func (k *KVStoreMock) Scan(*Range) ([]Value, error) {
	return make([]Value, 0), nil
}
