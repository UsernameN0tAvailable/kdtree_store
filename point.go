/**
point.go
Author: Tobias Famos & Mattia Pedrazzi
*/

package main

import (
	"errors"
	"math"
)

// as none value for partial queries
const None uint64 = math.MaxUint64

//type Key = []uint64
type Key = []uint64 // we need "optional values" for partial matches

type Point struct {
	isPartial bool
	mask []bool // if entry is true, not to consider for search
	coords Key
}

func NewPoint(c Key) Point {

	mask := make([]bool, len(c), len(c))
	coords := make(Key, len(c), len(c))

	isPartial := false

	for i, coord := range c {

		if isNone(coord) {
			mask[i] = true
			isPartial = true
		} else {
			coords[i] = coord
		}
	}

	return Point{isPartial: isPartial, mask: mask, coords: coords}
}


func (p *Point) GetSize() int {
	return len(p.coords)
}

func (p *Point) GetKeyAt(i int) (error, uint64) {
	if i >= p.GetSize() || i < 0 {
		return errors.New("key index out of range"), 0 
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

			tmp := k - p1k
			deltaSum += float64(tmp * tmp)
	
	}

	return nil, math.Sqrt(deltaSum)
}

func isNone(v uint64) bool {
	return v == None
}
