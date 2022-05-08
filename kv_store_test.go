/**
kv_store_test.go
Unit Tests for Key-Value Store
Author: Mattia Pedrazzi & Tobias Famos
*/
package main

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	STOREPATH   = "/tmp/store/"
	STORESIZE   = 2048
	letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

type KeyValuePair struct {
	key Point
	value [10]byte
}


func RandStringBytes(n int) [10]byte {
	b := [10]byte{}
	if n > 10 {
		n = 10
	}
	for i := 0; i < n; i++ {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return b
}

func TestNewKVStoreWithDefault(t *testing.T) {
	_, err := NewKVStore(&KVStoreOptions{})
	assert.NoError(t, err)
	// Check if folder exists on the file system
	_, err = os.Stat(STOREPATH)
	assert.False(t, os.IsNotExist(err))
}

func TestNewKVStoreWithFolder(t *testing.T) {
	_, err := NewKVStore(&KVStoreOptions{directory: STOREPATH})
	assert.NoError(t, err)
}

func TestNewKVStoreWithSize(t *testing.T) {
	_, err := NewKVStore(&KVStoreOptions{size: STORESIZE})
	assert.NoError(t, err)
}

func TestNewKVStore(t *testing.T) {
	_, err := NewKVStore(&KVStoreOptions{directory: STOREPATH, size: STORESIZE})
	assert.NoError(t, err)
}

func TestOpenKVStore(t *testing.T) {
	store, err := NewKVStore(&KVStoreOptions{directory: STOREPATH, size: STORESIZE})
	err = store.Open()
	assert.NoError(t, err)
}

func TestCloseKVStore(t *testing.T) {
	store, err := NewKVStore(&KVStoreOptions{directory: STOREPATH, size: STORESIZE})
	assert.NoError(t, err)
	assert.NoError(t, store.Open())
	assert.NoError(t, store.Close())
}

func TestDeleteKVStore(t *testing.T) {
	store, err := NewKVStore(&KVStoreOptions{directory: STOREPATH, size: STORESIZE})
	assert.NoError(t, err)
	assert.NoError(t, store.Delete())
	// Check if folder has been removed on the file system
	_, err = os.Stat(STOREPATH)
	assert.True(t, os.IsNotExist(err))
}

// TODO: maybe repeating the tests below with different dimensions would be a good idea

func TestPutKey(t *testing.T) {
	store, err := NewKVStore(&KVStoreOptions{directory: STOREPATH, size: STORESIZE})
	assert.NoError(t, err)
	data := RandStringBytes(10)

	point := NewPoint(Key{0, 0, 0})
	assert.NoError(t, store.Put(&point  , data))
}

func TestGetKey(t *testing.T) {
	store, err := NewKVStore(&KVStoreOptions{directory: STOREPATH, size: STORESIZE})
	assert.NoError(t, err)
	data := RandStringBytes(10)

	point := NewPoint(Key{0,0,0})

	assert.NoError(t, store.Put(&point, data))
	if result, err := store.Get(&point); assert.NoError(t, err) {
		assert.Equal(t, data, result)
	}
}

func TestUpsertKey(t *testing.T) {
	store, err := NewKVStore(&KVStoreOptions{directory: STOREPATH, size: STORESIZE})
	assert.NoError(t, err)

	point := NewPoint(Key{0, 0, 0})

	// Add a key first
	oldData := RandStringBytes(10)
	assert.NoError(t, store.Put(&point, oldData))
	// Update the key
	dataNew := RandStringBytes(10)
	assert.NoError(t, store.Upsert(&point, dataNew))
	// Check if update has worked
	if result, err := store.Get(&point); assert.NoError(t, err) {
		assert.Equal(t, dataNew, result)
	}
}

func TestScanRange3D(t *testing.T) {
	store, err := NewKVStore(&KVStoreOptions{directory: STOREPATH, size: STORESIZE})
	assert.NoError(t, err)
	// Add a key first
	oldData := RandStringBytes(10)

	// create and store points
	point_1 := NewPoint(Key{0, 0, 0})
	point_2 := NewPoint(Key{1, 1, 1})
	point_3 := NewPoint(Key{2, 2, 2})
	point_4 := NewPoint(Key{2, 3, 2})
	point_5 := NewPoint(Key{3, 3, 3})

	assert.NoError(t, store.Put(&point_1, oldData))
	assert.NoError(t, store.Put(&point_2, oldData))
	assert.NoError(t, store.Put(&point_3, oldData))
	assert.NoError(t, store.Put(&point_4, oldData))
	assert.NoError(t, store.Put(&point_5, oldData))


	entries, err := store.Scan(&Range{
		minKey: NewPoint(Key{1, 1, 1}),
		maxKey: NewPoint(Key{3, 3, 3}),
	})
	assert.NoError(t, err)
	// Check length of slice
	assert.Len(t, entries, 4)
}

func TestScanRange4D(t *testing.T) {
	store, err := NewKVStore(&KVStoreOptions{directory: STOREPATH, size: STORESIZE})
	assert.NoError(t, err)
	// Add a key first
	oldData := RandStringBytes(10)

	// create and store points
	point_1 := NewPoint(Key{0, 0, 0, 0})
	point_2 := NewPoint(Key{1, 1, 1, 1})
	point_3 := NewPoint(Key{2, 2, 2, 2})
	point_4 := NewPoint(Key{2, 3, 2, 2})
	point_5 := NewPoint(Key{3, 3, 3, 3})

	assert.NoError(t, store.Put(&point_1, oldData))
	assert.NoError(t, store.Put(&point_2, oldData))
	assert.NoError(t, store.Put(&point_3, oldData))
	assert.NoError(t, store.Put(&point_4, oldData))
	assert.NoError(t, store.Put(&point_5, oldData))


	entries, err := store.Scan(&Range{
		minKey: NewPoint(Key{1, 1, 1, 1}),
		maxKey: NewPoint(Key{3, 3, 3, 3}),
	})
	assert.NoError(t, err)
	// Check length of slice
	assert.Len(t, entries, 4)
}

func TestScanGTRange3D(t *testing.T) {
	store, err := NewKVStore(&KVStoreOptions{directory: STOREPATH, size: STORESIZE})
	assert.NoError(t, err)
	// Add a key first
	oldData := RandStringBytes(10)

	// create and store points
	point_1 := NewPoint(Key{0, 0, 0})
	point_2 := NewPoint(Key{1, 1, 1})
	point_3 := NewPoint(Key{2, 2, 2})
	point_4 := NewPoint(Key{2, 3, 2})
	point_5 := NewPoint(Key{3, 3, 3})

	assert.NoError(t, store.Put(&point_1, oldData))
	assert.NoError(t, store.Put(&point_2, oldData))
	assert.NoError(t, store.Put(&point_3, oldData))
	assert.NoError(t, store.Put(&point_4, oldData))
	assert.NoError(t, store.Put(&point_5, oldData))

	r := Range{
		minKey: NewPoint(Key{2, 2, 2}),
	}

	fmt.Println(r)

	entries, err := store.Scan(&r)

	assert.NoError(t, err)
	// Check length of slice
	assert.Len(t, entries, 3)
}


func TestScanGTRange4D(t *testing.T) {
	store, err := NewKVStore(&KVStoreOptions{directory: STOREPATH, size: STORESIZE})
	assert.NoError(t, err)
	// Add a key first
	oldData := RandStringBytes(10)

	// create and store points
	point_1 := NewPoint(Key{0, 0, 0, 0})
	point_2 := NewPoint(Key{1, 1, 1, 1})
	point_3 := NewPoint(Key{2, 2, 2, 2})
	point_4 := NewPoint(Key{2, 3, 2, 2})
	point_5 := NewPoint(Key{3, 3, 3, 3})

	assert.NoError(t, store.Put(&point_1, oldData))
	assert.NoError(t, store.Put(&point_2, oldData))
	assert.NoError(t, store.Put(&point_3, oldData))
	assert.NoError(t, store.Put(&point_4, oldData))
	assert.NoError(t, store.Put(&point_5, oldData))

	r := Range{
		minKey: NewPoint(Key{2, 2, 2, 2}),
	}

	fmt.Println(r)

	entries, err := store.Scan(&r)

	assert.NoError(t, err)
	// Check length of slice
	assert.Len(t, entries, 3)
}


func TestScanLERange3D(t *testing.T) {
	store, err := NewKVStore(&KVStoreOptions{directory: STOREPATH, size: STORESIZE})
	assert.NoError(t, err)
	// Add a key first
	oldData := RandStringBytes(10)

	// create and store points
	point1 := NewPoint(Key{0, 0, 0})
	point2 := NewPoint(Key{1, 1, 1})
	point3 := NewPoint(Key{2, 2, 2})
	point4 := NewPoint(Key{2, 3, 2})
	point5 := NewPoint(Key{3, 3, 3})

	assert.NoError(t, store.Put(&point1, oldData))
	assert.NoError(t, store.Put(&point2, oldData))
	assert.NoError(t, store.Put(&point3, oldData))
	assert.NoError(t, store.Put(&point4, oldData))
	assert.NoError(t, store.Put(&point5, oldData))

	entries, err := store.Scan(&Range{
		maxKey: NewPoint(Key{2, 2, 2}),
	})
	assert.NoError(t, err)
	// Check length of slice
	assert.Len(t, entries, 3)
}


func TestScanLERange4D(t *testing.T) {
	store, err := NewKVStore(&KVStoreOptions{directory: STOREPATH, size: STORESIZE})
	assert.NoError(t, err)
	// Add a key first
	oldData := RandStringBytes(10)

	// create and store points
	point1 := NewPoint(Key{0, 0, 0, 0})
	point2 := NewPoint(Key{1, 1, 1, 1})
	point3 := NewPoint(Key{2, 2, 2, 1})
	point4 := NewPoint(Key{2, 3, 2, 2})
	point5 := NewPoint(Key{3, 3, 3, 3})

	assert.NoError(t, store.Put(&point1, oldData))
	assert.NoError(t, store.Put(&point2, oldData))
	assert.NoError(t, store.Put(&point3, oldData))
	assert.NoError(t, store.Put(&point4, oldData))
	assert.NoError(t, store.Put(&point5, oldData))

	entries, err := store.Scan(&Range{
		maxKey: NewPoint(Key{2, 2, 2, 2}),
	})
	assert.NoError(t, err)
	// Check length of slice
	assert.Len(t, entries, 3)
}


func TestGetNN3D(t *testing.T) {
	store, err := NewKVStore(&KVStoreOptions{directory: STOREPATH, size: STORESIZE})
	assert.NoError(t, err)

	toSearch, toFind, toStore := createValues(3, 50) // 4D and 50 values stored

	for _, kv := range toStore {
		assert.NoError(t, store.Put(&kv.key, kv.value))
	}

	if result, err := store.GetNN(&toSearch.key); assert.NoError(t, err) {
		assert.Equal(t, toFind.value, result)
	}
}



func TestGetNN2D(t *testing.T) {
	store, err := NewKVStore(&KVStoreOptions{directory: STOREPATH, size: STORESIZE})
	assert.NoError(t, err)

	toSearch, toFind, toStore := createValues(4, 20) // 4D and 20 values stored

	for _, kv := range toStore {
		assert.NoError(t, store.Put(&kv.key, kv.value))
	}

	if result, err := store.GetNN(&toSearch.key); assert.NoError(t, err) {
		assert.Equal(t, toFind.value, result)
	}
}


func TestGetNN10D(t *testing.T) {
	store, err := NewKVStore(&KVStoreOptions{directory: STOREPATH, size: STORESIZE})
	assert.NoError(t, err)

	toSearch, toFind, toStore := createValues(10, 100) // 4D and 50 values stored

	for _, kv := range toStore {
		assert.NoError(t, store.Put(&kv.key, kv.value))
	}

	if result, err := store.GetNN(&toSearch.key); assert.NoError(t, err) {
		assert.Equal(t, toFind.value, result)
	}
}


// create values for different dimensions
// first return value is the value to search
// second is the nearest neighbour
func createValues(dimensions int, keyValuePairsCount int) (*KeyValuePair, *KeyValuePair, []KeyValuePair) {

	keyValuePairs := make([]KeyValuePair, keyValuePairsCount)

	for i := 0; i < keyValuePairsCount; i++ {
		data := RandStringBytes(10)
		key := make(Key, dimensions)

		for d := 0; d < dimensions; d++ {
			key[d] = randomUint64()
		}

		keyValuePairs[i] = KeyValuePair{key: NewPoint(key), value: data}
	}

	min := math.MaxFloat64

	toSearch := keyValuePairs[0]
	var nearest *KeyValuePair = nil

	for _, kv := range keyValuePairs[1:] {
		_, distance := toSearch.key.GetDistance(&kv.key)

		if distance < min {
			min = distance
			nearest = &kv
		}
	}

	return &toSearch, nearest, keyValuePairs[1:]
}

func randomUint64() uint64 {
    return uint64(rand.Uint32())<<32 + uint64(rand.Uint32())
}



