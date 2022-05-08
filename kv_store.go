/**
kv_store.go
Author: Tobias Famos & Mattia Pedrazzi
*/

package main

import (
	"path/filepath"
)


type KVStoreOptions struct {
	kSize int
	directory string
	size      int
}

type Range struct {
	minKey Point
	maxKey Point
}

type KVStoreManager interface {
	NewKVStore(options *KVStoreOptions) (KVStore, error)
}

type KVStore interface {
	Open() error
	Close() error
	Delete() error 
	GetNN(key *Point) ([10]byte, error) // nearest neighbour
	Get(key *Point) ([10]byte, error)
	Put(key *Point, value [10]byte) error
	Upsert(key *Point, value [10]byte) error
	Scan(options *Range) ([][10]byte, error)
}

type MockedKVStore struct {
	kSize int
	filepath string
	size     int
	// TODO
        // tree KDTree	
}

func NewKVStore(options *KVStoreOptions) (KVStore, error) {
	if options.size == 0 {
		options.size = 2048
	}
	return &MockedKVStore{
		kSize: options.kSize,
		filepath: filepath.Dir(options.directory),
		size:     options.size,
	}, nil
}

func (k *MockedKVStore) Open() error {
	return nil
}

func (k *MockedKVStore) Close() error {
	return nil
}

func (k *MockedKVStore) Delete() error {
	return nil
}

func (k *MockedKVStore) Get(key *Point) ([10]byte, error) {
	return [10]byte{}, nil
}

func (k *MockedKVStore) GetNN(key *Point) ([10]byte, error) {
	return [10]byte{}, nil
}

func (k *MockedKVStore) Put(key *Point, value [10]byte) error {
	return nil
}

func (k *MockedKVStore) Upsert(key *Point, value [10]byte) error {
	return nil
}

func (k *MockedKVStore) Scan(*Range) ([][10]byte, error) {
	return make([][10]byte, 0), nil
}
