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
	STORESIZE = 2048
	letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
)

type KeyValuePair struct {
	key Point
	value [10]byte
}


func RandString() Value  {
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
	assert.NoError(t, store.Put(&point  , data))
}

func TestPutWrongKey(t *testing.T) {
	store, err := NewKDTree(4, STORESIZE)
	assert.NoError(t, err)
	data := RandString()

	point := NewPoint(Key{UInt64(0), UInt64(0), UInt64(0)})
	assert.Error(t, store.Put(&point  , data))
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

			assert.NoError(t, store.Put(&point  , data))
		}
	}
	func TestGetKey(t *testing.T) {
		store, err := NewKDTree(3, STORESIZE)
		assert.NoError(t, err)
		data := RandString()

		point := NewPoint(Key{UInt64(0),UInt64(0),UInt64(0)})

		assert.NoError(t, store.Put(&point, data))


		if result, err := store.Get(&point); assert.NoError(t, err) {
			assert.Len(t, result, 1)
			assert.Equal(t, data, result)
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

				assert.NoError(t, store.Put(&point  , data))
			}	

			if result, err := store.Get(keyToSearch); assert.NoError(t, err) {
				assert.Len(t, result, 1)
				assert.Equal(t, valueToFind, result)
			}
		}

func TestDeleteLeafNode(t *testing.T) {
	rand.Seed(12)
	store, err := NewKDTree(10, STORESIZE)
	assert.NoError(t, err)

	var keyToDelete *Point = nil
	//valueToFind := ""

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

		if i == 49 {
			keyToDelete = &point
		}

		assert.NoError(t, store.Put(&point, data))
	}

	fmt.Println(store.GetNodesCount())

	if err := store.Delete(keyToDelete); assert.NoError(t, err) {
		assert.NoError(t, err)
		assert.Equal(t, 49, store.GetNodesCount())
	}
}

func TestDeleteKeyWithMultiples(t *testing.T) {
	rand.Seed(12)
	store, err := NewKDTree(10, STORESIZE)
	assert.NoError(t, err)

		var keyToDelete *Point = nil
		//valueToFind := ""

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
					keyToDelete = &point
				}

				assert.NoError(t, store.Put(&point  , data))
			}	


			fmt.Println(store.GetNodesCount())

			if err := store.Delete(keyToDelete); assert.NoError(t, err) {
				assert.NoError(t, err)
				assert.Equal(t, 49, store.GetNodesCount())
			}
		}



		func TestUpsertKey(t *testing.T) {
			store, err := NewKVStore(&KVStoreOptions{maxSize: STORESIZE, kSize: 3})
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
				assert.Equal(t, dataNew, result)
			}	}

			func TestScanRange3D(t *testing.T) {
				store, err := NewKVStore(&KVStoreOptions{maxSize: STORESIZE, kSize: 3})
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


				entries, err := store.Scan(&Range{
					minKey: NewPoint(Key{UInt64(1), UInt64(1), UInt64(1)}),
					maxKey: NewPoint(Key{UInt64(3), UInt64(3), UInt64(3)}),
				})
				assert.NoError(t, err)
				// Check length of slice
				assert.Len(t, entries, 4)
			}

			func TestScanRange4D(t *testing.T) {
				store, err := NewKVStore(&KVStoreOptions{maxSize: STORESIZE, kSize: 4})
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


				entries, err := store.Scan(&Range{
					minKey: NewPoint(Key{UInt64(1), UInt64(1), UInt64(1), UInt64(1)}),
					maxKey: NewPoint(Key{UInt64(3), UInt64(3), UInt64(3), UInt64(3)}),
				})
				assert.NoError(t, err)
				// Check length of slice
				assert.Len(t, entries, 4)
			}

			func TestScanGTRange3D(t *testing.T) {
				store, err := NewKVStore(&KVStoreOptions{maxSize: STORESIZE, kSize: 3})
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

				r := Range{
					minKey: NewPoint(Key{UInt64(2), UInt64(2), UInt64(2)}),
				}

				fmt.Println(r)

				entries, err := store.Scan(&r)

				assert.NoError(t, err)
				// Check length of slice
				assert.Len(t, entries, 3)
			}


			func TestScanGTRange4D(t *testing.T) {
				store, err := NewKVStore(&KVStoreOptions{maxSize: STORESIZE, kSize: 4})
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

				r := Range{
					minKey: NewPoint(Key{UInt64(2), UInt64(2), UInt64(2), UInt64(2)}),
				}

				fmt.Println(r)

				entries, err := store.Scan(&r)

				assert.NoError(t, err)
				// Check length of slice
				assert.Len(t, entries, 3)
			}


			func TestScanLERange3D(t *testing.T) {
				store, err := NewKVStore(&KVStoreOptions{maxSize: STORESIZE, kSize: 3})
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

				r := Range{
					minKey: NewPoint(Key{UInt64(2), UInt64(2), UInt64(2)}),
				}

				entries, err := store.Scan(&r)

				assert.NoError(t, err)
				// Check length of slice
				assert.Len(t, entries, 3)
			}

			func TestScanLERange4D(t *testing.T) {
				store, err := NewKVStore(&KVStoreOptions{maxSize: STORESIZE, kSize: 4})
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

				entries, err := store.Scan(&Range{
					maxKey: NewPoint(Key{UInt64(2), UInt64(2), UInt64(2), UInt64(2)}),
				})

				assert.NoError(t, err)
				// Check length of slice
				assert.Len(t, entries, 3)
			}


			func TestPartialGet4D(t *testing.T) {
				store, err := NewKVStore(&KVStoreOptions{maxSize: STORESIZE, kSize: 4})
				assert.NoError(t, err)
				// Add a key first
				oldData := RandString()

				// create and store points
				point1 := NewPoint(Key{UInt64(0), UInt64(0), UInt64(0), UInt64(0)})
				point2 := NewPoint(Key{UInt64(1), UInt64(1), UInt64(1), UInt64(1)})
				point3 := NewPoint(Key{UInt64(2), UInt64(2), UInt64(2), UInt64(2)})
				point4 := NewPoint(Key{UInt64(2), UInt64(2), UInt64(2), UInt64(2)})
				point5 := NewPoint(Key{UInt64(3), UInt64(3), UInt64(3), UInt64(3)})

				assert.NoError(t, store.Put(&point1, oldData))
				assert.NoError(t, store.Put(&point2, oldData))
				assert.NoError(t, store.Put(&point3, oldData))
				assert.NoError(t, store.Put(&point4, oldData))
				assert.NoError(t, store.Put(&point5, oldData))

				searchPoint := NewPoint(Key{UInt64(2), None(), UInt64(2), None()})

				entries, err := store.Get(&searchPoint)

				assert.NoError(t, err)
				// Check length of slice
				assert.Len(t, entries, 2)
			}


			func TestPartialGet3D(t *testing.T) {
				store, err := NewKVStore(&KVStoreOptions{maxSize: STORESIZE, kSize: 3})
				assert.NoError(t, err)
				// Add a key first
				oldData := RandString()

				// create and store points
				point1 := NewPoint(Key{UInt64(0), UInt64(0), UInt64(0)})
				point2 := NewPoint(Key{UInt64(1), UInt64(1), UInt64(1)})
				point3 := NewPoint(Key{UInt64(1), UInt64(2), UInt64(2)})
				point4 := NewPoint(Key{UInt64(2), UInt64(3), UInt64(2)})
				point5 := NewPoint(Key{UInt64(1), UInt64(3), UInt64(3)})

				assert.NoError(t, store.Put(&point1, oldData))
				assert.NoError(t, store.Put(&point2, oldData))
				assert.NoError(t, store.Put(&point3, oldData))
				assert.NoError(t, store.Put(&point4, oldData))
				assert.NoError(t, store.Put(&point5, oldData))

				searchPoint := NewPoint(Key{UInt64(1), None(), None(), None()})

				entries, err := store.Get(&searchPoint)

				assert.NoError(t, err)
				// Check length of slice
				assert.Len(t, entries, 3)
			}


			func TestGetNN3D(t *testing.T) {
				store, err := NewKVStore(&KVStoreOptions{maxSize: STORESIZE, kSize: 3})
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
				store, err := NewKVStore(&KVStoreOptions{maxSize: STORESIZE, kSize: 2})
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
				store, err := NewKVStore(&KVStoreOptions{maxSize: STORESIZE, kSize: 10})
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
					data := RandString()
					key := make(Key, dimensions)

					for d := 0; d < dimensions; d++ {
						key[d] = UInt64(randomUint64())
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



