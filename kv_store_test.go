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
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	STORESIZE   = 2048
	letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

type KeyValuePair struct {
	key   Point
	value [10]byte
}

func RandString() Value {
	out := new(Value)
	//n := rand.Intn(40) + 10

	for i := 0; i < 10; i++ {
		out[i] = letterBytes[rand.Intn(len(letterBytes))]
	}

	return *out
}

func TestNewKVStor(t *testing.T) {
	_, err := NewKVStore(&KVStoreOptions{maxSize: STORESIZE, kSize: 2})
	assert.NoError(t, err)
}

func TestPutKey(t *testing.T) {
	store, err := NewKDTree(3, STORESIZE)
	assert.NoError(t, err)
	data := RandString()

	point := NewPoint(Key{UInt64(0), UInt64(0), UInt64(0)})
	assert.NoError(t, store.Put(&point, data))
}

func TestPutWrongKey(t *testing.T) {
	store, err := NewKDTree(4, STORESIZE)
	assert.NoError(t, err)
	data := RandString()

	point := NewPoint(Key{UInt64(0), UInt64(0), UInt64(0)})
	assert.Error(t, store.Put(&point, data))
}

func TestPutMultiples(t *testing.T) {
	store, err := NewKDTree(5, STORESIZE)
	assert.NoError(t, err)

	for i := 0; i < 50; i++ {
		data := RandString()
		point := NewPoint(
			Key{
				UInt64(uint64(rand.Intn(40))),
				UInt64(uint64(rand.Intn(40))),
				UInt64(uint64(rand.Intn(40))),
				UInt64(uint64(rand.Intn(40))),
				UInt64(uint64(rand.Intn(40))),
			})

		assert.NoError(t, store.Put(&point, data))
	}
}
func TestGetKey(t *testing.T) {
	store, err := NewKDTree(3, STORESIZE)
	assert.NoError(t, err)
	data := RandString()

	point := NewPoint(Key{UInt64(0), UInt64(0), UInt64(0)})

	assert.NoError(t, store.Put(&point, data))

	if result, err := store.Get(&point); assert.NoError(t, err) {
		assert.Len(t, result, 1)
		assert.Equal(t, data, result[0])
	}
}

func TestGetKeyWithMultiples(t *testing.T) {

	store, err := NewKDTree(10, STORESIZE)
	assert.NoError(t, err)

	var keyToSearch *Point = nil
	var valueToFind Value

	for i := 0; i < 50; i++ {
		data := RandString()
		point := NewPoint(
			Key{
				UInt64(uint64(rand.Intn(40))),
				UInt64(uint64(rand.Intn(40))),
				UInt64(uint64(rand.Intn(40))),
				UInt64(uint64(rand.Intn(40))),
				UInt64(uint64(rand.Intn(40))),
				UInt64(uint64(rand.Intn(40))),
				UInt64(uint64(rand.Intn(40))),
				UInt64(uint64(rand.Intn(40))),
				UInt64(uint64(rand.Intn(40))),
				UInt64(uint64(rand.Intn(40))),
			})

		if i == 20 {
			keyToSearch = &point
			valueToFind = data
		}

		assert.NoError(t, store.Put(&point, data))
	}

	if result, err := store.Get(keyToSearch); assert.NoError(t, err) {
		assert.Len(t, result, 1)
		assert.Equal(t, valueToFind, result[0])
	}
}

func TestDeleteLeafNode(t *testing.T) {
	runDeletionTest(t, 49)
}

func TestDeleteNodeWithOnlyLeftSubTree(t *testing.T) {
	runDeletionTest(t, 7)
}

func TestDeleteKeyWithMultiples(t *testing.T) {
	runDeletionTest(t, 20)
}

func TestDeleteRootNode(t *testing.T) {
	runDeletionTest(t, 0)
}

func TestDeleteIndex8(t *testing.T) {
	runDeletionTest(t, 8)
}

func runDeletionTest(t *testing.T, indexToDelete int) {
	rand.Seed(12)
	store, err := NewKDTree(10, STORESIZE)
	assert.NoError(t, err)

	var keyToDelete *Point = nil
	var valueToDelete Value
	fmt.Println(valueToDelete)
	//valueToFind := ""
	var allKeys []Point

	for i := 0; i < 50; i++ {
		data := RandString()
		point := NewPoint(
			Key{
				UInt64(uint64(rand.Intn(40))),
				UInt64(uint64(rand.Intn(40))),
				UInt64(uint64(rand.Intn(40))),
				UInt64(uint64(rand.Intn(40))),
				UInt64(uint64(rand.Intn(40))),
				UInt64(uint64(rand.Intn(40))),
				UInt64(uint64(rand.Intn(40))),
				UInt64(uint64(rand.Intn(40))),
				UInt64(uint64(rand.Intn(40))),
				UInt64(uint64(rand.Intn(40))),
			})
		if i == indexToDelete {
			keyToDelete = &point
			valueToDelete = data
		} else {
			allKeys = append(allKeys, point)
		}

		assert.NoError(t, store.Put(&point, data))
	}

	fmt.Println(store.GetNodesCount())

	if err := store.Delete(keyToDelete); assert.NoError(t, err) {
		assert.NoError(t, err)
		assert.Equal(t, 49, store.GetNodesCount())
	}
	_, err = store.Get(keyToDelete)
	assert.Error(t, err, "No error thrown")

	for index := 0; index < len(allKeys); index++ {
		_, err := store.Get(&allKeys[index])
		assert.NoError(t, err)
	}
}

func TestUpsert(t *testing.T) {
	store, err := NewKDTree(3, STORESIZE)
	assert.NoError(t, err)

	point := NewPoint(Key{UInt64(0), UInt64(0), UInt64(0)})

	// Add a key first
	oldData := RandString()
	assert.NoError(t, store.Put(&point, oldData))

	// Update the key
	dataNew := RandString()
	assert.NoError(t, store.Upsert(&point, dataNew))
	// Check if update has worked
	if result, err := store.Get(&point); assert.NoError(t, err) {
		assert.Len(t, result, 1)
		assert.Equal(t, dataNew, result[0])
	}
}

func TestScanRange3D(t *testing.T) {

	store, err := NewKDTree(3, STORESIZE)
	assert.NoError(t, err)
	// Add a key first
	oldData := RandString()

	// create and store points
	point1 := NewPoint(Key{UInt64(0), UInt64(0), UInt64(0)})
	point2 := NewPoint(Key{UInt64(1), UInt64(1), UInt64(1)})
	point3 := NewPoint(Key{UInt64(2), UInt64(2), UInt64(2)})
	point4 := NewPoint(Key{UInt64(2), UInt64(3), UInt64(2)})
	point5 := NewPoint(Key{UInt64(3), UInt64(3), UInt64(3)})

	assert.NoError(t, store.Put(&point1, oldData))
	assert.NoError(t, store.Put(&point2, oldData))
	assert.NoError(t, store.Put(&point3, oldData))
	assert.NoError(t, store.Put(&point4, oldData))
	assert.NoError(t, store.Put(&point5, oldData))

	from := NewPoint(Key{UInt64(1), UInt64(1), UInt64(1)})
	to := NewPoint(Key{UInt64(3), UInt64(3), UInt64(3)})

	entries, err := store.Scan(&from, &to)

	assert.NoError(t, err)
	// Check length of slice
	assert.Len(t, entries, 4)
}

func TestScanRange4D(t *testing.T) {

	store, err := NewKDTree(4, STORESIZE)
	assert.NoError(t, err)
	// Add a key first
	oldData := RandString()

	// create and store points
	point1 := NewPoint(Key{UInt64(0), UInt64(0), UInt64(0), UInt64(0)})
	point2 := NewPoint(Key{UInt64(1), UInt64(1), UInt64(1), UInt64(1)})
	point3 := NewPoint(Key{UInt64(2), UInt64(2), UInt64(2), UInt64(2)})
	point4 := NewPoint(Key{UInt64(2), UInt64(3), UInt64(2), UInt64(2)})
	point5 := NewPoint(Key{UInt64(3), UInt64(3), UInt64(3), UInt64(3)})

	assert.NoError(t, store.Put(&point1, oldData))
	assert.NoError(t, store.Put(&point2, oldData))
	assert.NoError(t, store.Put(&point3, oldData))
	assert.NoError(t, store.Put(&point4, oldData))
	assert.NoError(t, store.Put(&point5, oldData))

	from := NewPoint(Key{UInt64(1), UInt64(1), UInt64(1), UInt64(1)})
	to := NewPoint(Key{UInt64(3), UInt64(3), UInt64(3), UInt64(3)})

	entries, err := store.Scan(&from, &to)

	assert.NoError(t, err)
	// Check length of slice
	assert.Len(t, entries, 4)
}

func TestScanGTRange3D(t *testing.T) {
	store, err := NewKDTree(3, STORESIZE)
	assert.NoError(t, err)
	// Add a key first
	oldData := RandString()
	// create and store points

	// create and store points
	point1 := NewPoint(Key{UInt64(0), UInt64(0), UInt64(0)})
	point2 := NewPoint(Key{UInt64(1), UInt64(1), UInt64(1)})
	point3 := NewPoint(Key{UInt64(2), UInt64(2), UInt64(2)})
	point4 := NewPoint(Key{UInt64(2), UInt64(3), UInt64(2)})
	point5 := NewPoint(Key{UInt64(3), UInt64(3), UInt64(3)})

	assert.NoError(t, store.Put(&point1, oldData))
	assert.NoError(t, store.Put(&point2, oldData))
	assert.NoError(t, store.Put(&point3, oldData))
	assert.NoError(t, store.Put(&point4, oldData))
	assert.NoError(t, store.Put(&point5, oldData))

	from := NewPoint(Key{UInt64(2), UInt64(2), UInt64(2)})

	entries, err := store.Scan(&from, nil)

	assert.NoError(t, err)
	// Check length of slice
	assert.Len(t, entries, 3)
}

func TestScanGTRange4D(t *testing.T) {

	store, err := NewKDTree(4, STORESIZE)
	assert.NoError(t, err)
	// Add a key first
	oldData := RandString()

	// create and store points
	point1 := NewPoint(Key{UInt64(0), UInt64(0), UInt64(0), UInt64(0)})
	point2 := NewPoint(Key{UInt64(1), UInt64(1), UInt64(1), UInt64(1)})
	point3 := NewPoint(Key{UInt64(2), UInt64(2), UInt64(2), UInt64(2)})
	point4 := NewPoint(Key{UInt64(2), UInt64(3), UInt64(2), UInt64(2)})
	point5 := NewPoint(Key{UInt64(3), UInt64(3), UInt64(3), UInt64(3)})

	assert.NoError(t, store.Put(&point1, oldData))
	assert.NoError(t, store.Put(&point2, oldData))
	assert.NoError(t, store.Put(&point3, oldData))
	assert.NoError(t, store.Put(&point4, oldData))
	assert.NoError(t, store.Put(&point5, oldData))

	from := NewPoint(Key{UInt64(2), UInt64(2), UInt64(2), UInt64(2)})

	entries, err := store.Scan(&from, nil)

	assert.NoError(t, err)
	// Check length of slice
	assert.Len(t, entries, 3)
}

func TestScanLERange3D(t *testing.T) {
	store, err := NewKDTree(3, STORESIZE)
	assert.NoError(t, err)
	// Add a key first
	oldData := RandString()

	// create and store points
	point1 := NewPoint(Key{UInt64(0), UInt64(0), UInt64(0)})
	point2 := NewPoint(Key{UInt64(1), UInt64(1), UInt64(1)})
	point3 := NewPoint(Key{UInt64(2), UInt64(2), UInt64(2)})
	point4 := NewPoint(Key{UInt64(2), UInt64(3), UInt64(2)})
	point5 := NewPoint(Key{UInt64(3), UInt64(3), UInt64(3)})

	assert.NoError(t, store.Put(&point1, oldData))
	assert.NoError(t, store.Put(&point2, oldData))
	assert.NoError(t, store.Put(&point3, oldData))
	assert.NoError(t, store.Put(&point4, oldData))
	assert.NoError(t, store.Put(&point5, oldData))

	to := NewPoint(Key{UInt64(2), UInt64(2), UInt64(2)})

	entries, err := store.Scan(nil, &to)

	assert.NoError(t, err)
	// Check length of slice
	assert.Len(t, entries, 3)
}

func TestScanLERange4D(t *testing.T) {
	store, err := NewKDTree(4, STORESIZE)
	assert.NoError(t, err)
	// Add a key first
	oldData := RandString()

	// create and store points
	point1 := NewPoint(Key{UInt64(0), UInt64(0), UInt64(0), UInt64(0)})
	point2 := NewPoint(Key{UInt64(1), UInt64(1), UInt64(1), UInt64(1)})
	point3 := NewPoint(Key{UInt64(2), UInt64(2), UInt64(2), UInt64(2)})
	point4 := NewPoint(Key{UInt64(2), UInt64(3), UInt64(2), UInt64(2)})
	point5 := NewPoint(Key{UInt64(3), UInt64(3), UInt64(3), UInt64(3)})

	assert.NoError(t, store.Put(&point1, oldData))
	assert.NoError(t, store.Put(&point2, oldData))
	assert.NoError(t, store.Put(&point3, oldData))
	assert.NoError(t, store.Put(&point4, oldData))
	assert.NoError(t, store.Put(&point5, oldData))

	to := NewPoint(Key{UInt64(2), UInt64(2), UInt64(2), UInt64(2)})

	entries, err := store.Scan(nil, &to)

	assert.NoError(t, err)
	// Check length of slice
	assert.Len(t, entries, 3)
}

func TestPartialGet4D(t *testing.T) {
	store, err := NewKDTree(4, STORESIZE)
	assert.NoError(t, err)
	// Add a key first
	oldData := RandString()
	toFind1 := RandString()
	toFind2 := RandString()

	// create and store points
	point1 := NewPoint(Key{UInt64(0), UInt64(0), UInt64(0), UInt64(0)})
	point2 := NewPoint(Key{UInt64(1), UInt64(1), UInt64(1), UInt64(1)})
	point3 := NewPoint(Key{UInt64(2), UInt64(2), UInt64(2), UInt64(2)})
	point4 := NewPoint(Key{UInt64(2), UInt64(2), UInt64(2), UInt64(2)})
	point5 := NewPoint(Key{UInt64(3), UInt64(3), UInt64(3), UInt64(3)})

	assert.NoError(t, store.Put(&point1, oldData))
	assert.NoError(t, store.Put(&point2, oldData))
	assert.NoError(t, store.Put(&point3, toFind1))
	assert.NoError(t, store.Put(&point4, toFind2))
	assert.NoError(t, store.Put(&point5, oldData))

	searchPoint := NewPoint(Key{UInt64(2), None(), UInt64(2), None()})

	entries, err := store.Get(&searchPoint)

	assert.NoError(t, err)
	// Check length of slice
	assert.Len(t, entries, 2)
	assert.Equal(t, toFind1, entries[0])
	assert.Equal(t, toFind2, entries[1])
}

func TestPartialGet3D(t *testing.T) {
	store, err := NewKDTree(3, STORESIZE)
	assert.NoError(t, err)
	// Add a key first
	oldData := RandString()
	toFind1 := RandString()
	toFind2 := RandString()
	toFind3 := RandString()


	// create and store points
	point1 := NewPoint(Key{UInt64(0), UInt64(0), UInt64(0)})
	point2 := NewPoint(Key{UInt64(1), UInt64(1), UInt64(1)})
	point3 := NewPoint(Key{UInt64(1), UInt64(2), UInt64(2)})
	point4 := NewPoint(Key{UInt64(2), UInt64(3), UInt64(2)})
	point5 := NewPoint(Key{UInt64(1), UInt64(3), UInt64(3)})

	assert.NoError(t, store.Put(&point1, oldData))
	assert.NoError(t, store.Put(&point2, toFind1))
	assert.NoError(t, store.Put(&point3, toFind2))
	assert.NoError(t, store.Put(&point4, oldData))
	assert.NoError(t, store.Put(&point5, toFind3))

	searchPoint := NewPoint(Key{UInt64(1), None(), None()})

	entries, err := store.Get(&searchPoint)

	assert.NoError(t, err)
	// Check length of slice
	assert.Len(t, entries, 3)
	assert.Equal(t, toFind1, entries[0])
	assert.Equal(t, toFind2, entries[1])
	assert.Equal(t, toFind3, entries[2])


}

func TestGetNN3D(t *testing.T) {


	store, err := NewKDTree(3, STORESIZE)
	assert.NoError(t, err)

	toSearch, toFind, toStore := createValues(3, 50) // 4D and 20 values stored

	for _, kv := range toStore {
		assert.NoError(t, store.Put(&kv.key, kv.value))
	}

	if result, err := store.GetNN(&toSearch.key); assert.NoError(t, err) {
		assert.Equal(t, toFind.value, result)
	} 
}

func TestGetNN2D(t *testing.T) {

	store, err := NewKDTree(2, STORESIZE)
	assert.NoError(t, err)

	toSearch, toFind, toStore := createValues(2, 50) // 4D and 20 values stored

	for _, kv := range toStore {
		assert.NoError(t, store.Put(&kv.key, kv.value))
	}

	if result, err := store.GetNN(&toSearch.key); assert.NoError(t, err) {
		assert.Equal(t, toFind.value, result)
	}
}

func TestGetNN10D(t *testing.T) {

	store, err := NewKDTree(10, STORESIZE)
	assert.NoError(t, err)

	toSearch, toFind, toStore := createValues(10, 60) // 4D and 20 values stored

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
func createValues(dimensions int, keyValuePairsCount int) (KeyValuePair, KeyValuePair, []KeyValuePair) {

	keyValuePairs := make([]KeyValuePair, keyValuePairsCount)

	for i := 0; i < keyValuePairsCount; i++ {
		data := RandString()
		key := make(Key, dimensions)

		for d := 0; d < dimensions; d++ {
			key[d] = UInt64(randomUint64())
		}

		keyValuePairs[i] = KeyValuePair{key: NewPoint(key), value: data}
	}

	min := math.MaxFloat64

	toSearch := keyValuePairs[0]
	toStore := keyValuePairs[1:]
	var nearest KeyValuePair 

	for _, kv := range toStore {
		_, distance := toSearch.key.GetDistance(&kv.key)
		if distance < min {
			min = distance
			nearest = kv
		}
	}

	return toSearch, nearest, toStore
}

func randomUint64() uint64 {
	return uint64(rand.Uint32()) // avoid overflows!!
}
