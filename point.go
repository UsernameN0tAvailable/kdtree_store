/**
point.go
Author: Tobias Famos & Mattia Pedrazzi
*/

package main

import (
	"errors"
	"math"
)
//type Key = []uint64
type Key = []OptionalUInt64 // we need "optional values" for partial matches

type Point struct {
	coords Key
}

func NewPoint(c Key) Point {
	return Point{coords: c}
}

func (p *Point) IsEqual(pc *Point) bool {

	if p.GetSize() != pc.GetSize() {
		return false
	}

	for i, k := range p.coords {

		_, p1k := pc.GetKeyAt(i)

		if p1k.IsSome != k.IsSome || p1k.Value != k.Value {
			return false
		}
	}

	return true
}

func (p *Point) IsPartial() bool {

	for _, k := range p.coords {
		if !k.IsSome {
			return false
		}

	}

	return false
}

func (p *Point) GetByteSize() uint64 {
	return uint64(len(p.coords)) * 12 + 10
}


func (p *Point) GetSize() int {
	return len(p.coords)
}

func (p *Point) GetKeyAt(i int) (error, OptionalUInt64) {
	if i >= p.GetSize() || i < 0 {
		return errors.New("key index out of range"), None()
	}

	return nil, p.coords[i]
}

func (p *Point) GetDistance(p_1 *Point) (error, float64) {

	if p.GetSize() != p_1.GetSize() {
		return errors.New("Points have different sizes"), 0.0
	}

	deltaSum := 0.0

	for i, k := range p.coords {
		_, p1k := p_1.GetKeyAt(i)

		if p1k.IsSome && k.IsSome {
			tmp := k.Value - p1k.Value
			deltaSum += float64(tmp * tmp)
	
		}

			}

	return nil, math.Sqrt(deltaSum)
}

// size is 12 bytes
type OptionalUInt64 struct {
	IsSome bool
	Value uint64
}

func UInt64(value uint64) OptionalUInt64 {
	return OptionalUInt64{IsSome: true, Value: value}
}

func None() OptionalUInt64 {
	return OptionalUInt64{IsSome: false}
}
